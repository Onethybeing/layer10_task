"""
Deduplication & Canonicalization Pipeline
- Artifact dedup (near-duplicate issues)
- Entity canonicalization (persons, components)
- Claim dedup (merge repeated facts, keep all evidence)
- Conflict/revision tracking
- Fully reversible with merge audit log
"""

import json
import hashlib
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional
from datetime import datetime

DEDUP_DIR   = Path("outputs/dedup")
MERGE_LOG   = Path("outputs/merge_audit_log.json")


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def jaccard_similarity(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def text_tokens(text: str) -> set:
    return set(re.findall(r"\b\w{3,}\b", text.lower()))


# ── Component Aliases ─────────────────────────────────────────────────────────

COMPONENT_ALIASES: dict[str, list[str]] = {
    "editor":      ["text editor", "monaco", "code editor", "editor core"],
    "terminal":    ["integrated terminal", "terminal emulator", "shell"],
    "debugger":    ["debug", "debugging", "debug adapter", "launch config"],
    "extensions":  ["extension host", "marketplace", "plugin", "vsix"],
    "git":         ["source control", "scm", "git integration", "version control"],
    "search":      ["find", "search editor", "global search", "ripgrep"],
    "themes":      ["color theme", "icon theme", "theming", "syntax highlighting"],
    "keybindings": ["keyboard shortcuts", "hotkeys", "keymaps"],
    "settings":    ["configuration", "preferences", "settings.json", "workspace settings"],
    "notebook":    ["jupyter", "notebook editor", "interactive window"],
    "remote":      ["remote ssh", "remote containers", "remote wsl", "codespaces"],
    "accessibility": ["a11y", "screen reader", "aria"],
    "performance": ["memory", "cpu", "startup time", "latency"],
    "workbench":   ["activity bar", "sidebar", "panel", "status bar", "tabs"],
}

def canonicalize_component(raw: str) -> str:
    """Map raw component name → canonical component id."""
    norm = normalize_name(raw)
    for canonical, aliases in COMPONENT_ALIASES.items():
        if norm == canonical or norm in [normalize_name(a) for a in aliases]:
            return canonical
    # Fuzzy: if any alias token appears in norm
    for canonical, aliases in COMPONENT_ALIASES.items():
        all_terms = [canonical] + aliases
        for term in all_terms:
            if normalize_name(term) in norm or norm in normalize_name(term):
                return canonical
    return norm  # keep as-is if no match


# ── Entity Canonicalizer ──────────────────────────────────────────────────────

class EntityCanonicalizer:
    def __init__(self):
        self.persons: dict[str, dict] = {}       # canonical_id → person
        self.components: dict[str, dict] = {}    # canonical_id → component
        self.merge_log: list[dict] = []

    def add_person(self, login: str, display_name: Optional[str] = None) -> str:
        """Register a person. Returns canonical id."""
        canonical_id = f"github:{login.lower()}"
        if canonical_id not in self.persons:
            self.persons[canonical_id] = {
                "id": canonical_id,
                "type": "Person",
                "login": login.lower(),
                "display_name": display_name,
                "aliases": [],
            }
        elif display_name and not self.persons[canonical_id].get("display_name"):
            self.persons[canonical_id]["display_name"] = display_name
        return canonical_id

    def add_component(self, raw_name: str, description: Optional[str] = None) -> str:
        """Register a component. Returns canonical id."""
        canonical_slug = canonicalize_component(raw_name)
        canonical_id   = f"component:{canonical_slug}"

        if canonical_id not in self.components:
            self.components[canonical_id] = {
                "id": canonical_id,
                "type": "Component",
                "name": canonical_slug,
                "aliases": [],
                "description": description,
            }
        # Track alias
        norm = normalize_name(raw_name)
        if norm != canonical_slug and norm not in self.components[canonical_id]["aliases"]:
            self.components[canonical_id]["aliases"].append(norm)
            self.merge_log.append({
                "action": "alias_added",
                "entity_id": canonical_id,
                "alias": raw_name,
                "reason": "component_canonicalization",
            })

        return canonical_id

    def resolve_ref(self, ref: str, hint: str = "auto") -> str:
        """
        Resolve an entity ref string to a canonical id.
        hint: 'person', 'component', 'issue', or 'auto'
        """
        if not ref:
            return ref
        ref = ref.strip()

        # Already canonical
        if ref.startswith(("github:", "component:", "issue:", "label:")):
            return ref

        # Issue number ref
        if re.match(r"^\d+$", ref):
            return f"issue:{ref}"

        # Try as person login (alphanumeric + hyphens)
        if re.match(r"^[\w\-]+$", ref) and hint in ("person", "auto"):
            if f"github:{ref.lower()}" in self.persons:
                return f"github:{ref.lower()}"

        # Try as component
        if hint in ("component", "auto"):
            slug = canonicalize_component(ref)
            cid  = f"component:{slug}"
            if cid in self.components:
                return cid

        # Fallback: treat as component
        return f"component:{normalize_name(ref)}"

    def all_entities(self) -> list[dict]:
        return list(self.persons.values()) + list(self.components.values())


# ── Artifact Deduplicator ─────────────────────────────────────────────────────

class ArtifactDeduplicator:
    """Detect near-duplicate issues using token Jaccard similarity."""

    def __init__(self, threshold: float = 0.75):
        self.threshold = threshold
        self.seen: dict[str, set] = {}          # issue_id → token set
        self.duplicates: dict[str, str] = {}    # issue_id → canonical_issue_id

    def add_issue(self, issue_id: str, title: str, body: str):
        tokens = text_tokens(f"{title} {body[:500]}")
        self.seen[issue_id] = tokens

    def find_duplicates(self) -> dict[str, str]:
        """O(n²) pairwise check — fine for 200 issues."""
        ids = list(self.seen.keys())
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = ids[i], ids[j]
                if a in self.duplicates or b in self.duplicates:
                    continue
                sim = jaccard_similarity(self.seen[a], self.seen[b])
                if sim >= self.threshold:
                    # Keep lower issue number as canonical
                    num_a = int(a.split(":")[1])
                    num_b = int(b.split(":")[1])
                    canonical, dup = (a, b) if num_a < num_b else (b, a)
                    self.duplicates[dup] = canonical
        return self.duplicates


# ── Claim Deduplicator ────────────────────────────────────────────────────────

class ClaimDeduplicator:
    """
    Merge claims of the same type with the same subject+object.
    Keeps all evidence. Tracks conflicts and revisions.
    """

    def __init__(self):
        self.claims: dict[str, dict] = {}
        self.merge_log: list[dict] = []

    def _claim_key(self, claim: dict) -> str:
        value = claim.get('value') or ''
        object_id = claim.get('object_id') or ''
        return f"{claim['claim_type']}|{claim['subject_id']}|{object_id}|{value[:50]}"

    def add_claim(self, claim: dict) -> str:
        """Add claim, merging if a matching one exists. Returns canonical claim id."""
        key = self._claim_key(claim)

        # Find existing claim with same key
        existing_id = None
        for cid, existing in self.claims.items():
            if self._claim_key(existing) == key:
                existing_id = cid
                break

        if existing_id is None:
            self.claims[claim["id"]] = claim
            return claim["id"]

        # Merge: accumulate evidence, keep highest confidence
        existing = self.claims[existing_id]
        new_evidence = claim.get("evidence", [])
        existing_evidence = existing.get("evidence", [])

        # Dedup evidence by source_id + excerpt
        seen_evidence = {(e["source_id"], e["excerpt"][:50]) for e in existing_evidence}
        for ev in new_evidence:
            ev_key = (ev["source_id"], ev["excerpt"][:50])
            if ev_key not in seen_evidence:
                existing_evidence.append(ev)
                seen_evidence.add(ev_key)

        existing["evidence"] = existing_evidence
        existing["confidence"] = max(existing["confidence"], claim["confidence"])

        self.merge_log.append({
            "action": "claim_merged",
            "merged_claim_id": claim["id"],
            "into_claim_id": existing_id,
            "reason": "same_type_subject_object",
            "evidence_added": len(new_evidence),
        })

        return existing_id

    def detect_conflicts(self) -> list[dict]:
        """
        Find claims that contradict each other:
        - Same subject, same claim_type, different object/value
        """
        conflicts = []
        by_subject_type: dict[str, list] = defaultdict(list)
        for claim in self.claims.values():
            sk = f"{claim['claim_type']}|{claim['subject_id']}"
            by_subject_type[sk].append(claim)

        for key, group in by_subject_type.items():
            if len(group) < 2:
                continue
            # Check for conflicting values
            values = set(c.get("value", c.get("object_id", "")) for c in group)
            if len(values) > 1:
                conflicts.append({
                    "type": "conflicting_claims",
                    "key": key,
                    "claims": [c["id"] for c in group],
                    "values": list(values),
                })
        return conflicts

    def mark_superseded(self, old_claim_id: str, new_claim_id: str, reason: str):
        """Mark a claim as superseded (historical, no longer current)."""
        if old_claim_id in self.claims:
            self.claims[old_claim_id]["superseded_by"] = new_claim_id
            self.claims[old_claim_id]["valid_until"] = datetime.utcnow().isoformat() + "Z"
            self.merge_log.append({
                "action": "claim_superseded",
                "claim_id": old_claim_id,
                "superseded_by": new_claim_id,
                "reason": reason,
            })


# ── Main Dedup Pipeline ───────────────────────────────────────────────────────

def run_dedup_pipeline(
    issues: list[dict],
    extractions: list[dict],
) -> dict:
    """
    Full deduplication pass.
    Returns {entities, claims, artifact_dupes, conflicts, merge_log}
    """
    DEDUP_DIR.mkdir(parents=True, exist_ok=True)

    entity_canon  = EntityCanonicalizer()
    artifact_dedup = ArtifactDeduplicator(threshold=0.72)
    claim_dedup   = ClaimDeduplicator()

    print("\nRunning deduplication pipeline …")

    # ── Pass 1: Register issues for artifact dedup ────────────────────────────
    for issue in issues:
        artifact_dedup.add_issue(issue["id"], issue["title"], issue.get("body", ""))
        # Register issue author
        entity_canon.add_person(issue["user"])
        for assignee in issue.get("assignees", []):
            entity_canon.add_person(assignee)
        for comment in issue.get("comments", []):
            entity_canon.add_person(comment["user"])

    artifact_dupes = artifact_dedup.find_duplicates()
    print(f"  Artifact duplicates found: {len(artifact_dupes)}")

    # ── Pass 2: Process extractions ───────────────────────────────────────────
    for ext in extractions:
        issue_id = ext["issue_id"]

        # Register extracted entities
        for person in ext.get("entities", {}).get("persons", []):
            entity_canon.add_person(
                person.get("login", "unknown"),
                person.get("display_name"),
            )
        for component in ext.get("entities", {}).get("components", []):
            entity_canon.add_component(
                component.get("name", ""),
                component.get("description"),
            )

        # Resolve claim refs to canonical ids and add to deduplicator
        for claim in ext.get("claims", []):
            # Resolve subject
            subject_raw = claim.get("subject_id", "")
            claim["subject_id"] = entity_canon.resolve_ref(subject_raw)

            # Resolve object
            object_raw = claim.get("object_id")
            if object_raw:
                claim["object_id"] = entity_canon.resolve_ref(object_raw)

            # Add claim (will merge if duplicate)
            claim_dedup.add_claim(claim)

    # ── Pass 3: Detect conflicts ──────────────────────────────────────────────
    conflicts = claim_dedup.detect_conflicts()
    print(f"  Conflicting claim groups: {len(conflicts)}")

    # ── Merge logs ────────────────────────────────────────────────────────────
    full_merge_log = entity_canon.merge_log + claim_dedup.merge_log
    with open(MERGE_LOG, "w") as f:
        json.dump(full_merge_log, f, indent=2)

    result = {
        "entities":       entity_canon.all_entities(),
        "persons":        entity_canon.persons,
        "components":     entity_canon.components,
        "claims":         list(claim_dedup.claims.values()),
        "artifact_dupes": artifact_dupes,
        "conflicts":      conflicts,
        "merge_log":      full_merge_log,
        "stats": {
            "total_persons":    len(entity_canon.persons),
            "total_components": len(entity_canon.components),
            "total_claims":     len(claim_dedup.claims),
            "artifact_dupes":   len(artifact_dupes),
            "conflicts":        len(conflicts),
            "merge_events":     len(full_merge_log),
        }
    }

    # Save outputs
    with open(DEDUP_DIR / "entities.json", "w") as f:
        json.dump(result["entities"], f, indent=2)
    with open(DEDUP_DIR / "claims.json", "w") as f:
        json.dump(result["claims"], f, indent=2)
    with open(DEDUP_DIR / "artifact_dupes.json", "w") as f:
        json.dump(artifact_dupes, f, indent=2)
    with open(DEDUP_DIR / "conflicts.json", "w") as f:
        json.dump(conflicts, f, indent=2)

    print(f"\nDedup stats: {result['stats']}")
    return result
