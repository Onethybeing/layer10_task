# Layer10 Take-Home — Grounded Long-Term Memory via Structured Extraction

**Corpus:** `microsoft/vscode` GitHub Issues  
**Stack:** Gemini 1.5 Flash · Neo4j Aura · Qdrant · Python 3.11+

---

## Architecture Overview

```
GitHub Issues API
      │
      ▼
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│  fetcher.py │────▶│ Raw Issues + Comments (outputs/raw_issues/*.json) │
└─────────────┘     └──────────────────────────────────────────────────┘
      │
      ▼
┌──────────────┐    ┌───────────────────────────────────────────────────┐
│ extractor.py │───▶│ Entities + Claims + Evidence (outputs/extractions/)│
│  (Gemini)    │    │ Every claim → evidence {source_id, excerpt, offset}│
└──────────────┘    └───────────────────────────────────────────────────┘
      │
      ▼
┌─────────┐         ┌────────────────────────────────────────────────────┐
│ dedup.py │────────▶│ Canonical entities, merged claims, audit log       │
└─────────┘         └────────────────────────────────────────────────────┘
      │
      ├──────────────────────────────────┐
      ▼                                  ▼
┌───────────────┐                 ┌─────────────┐
│ graph_builder │                 │ retrieval.py │
│   Neo4j Aura  │                 │   Qdrant     │
│               │                 │              │
│ Nodes:        │                 │ Embeds:      │
│  Issue        │                 │  Issues      │
│  Person       │                 │  Claims      │
│  Component    │                 │              │
│  Label        │                 │ Semantic     │
│  Claim        │                 │ search →     │
│  Evidence     │                 │ context packs│
└───────────────┘                 └─────────────┘
      │                                  │
      └──────────────┬───────────────────┘
                     ▼
              ┌──────────┐
              │  api.py  │  Flask REST API (port 5000)
              └──────────┘
                     │
                     ▼
              ┌─────────────────┐
              │ viz/index.html  │  Interactive Graph Explorer
              └─────────────────┘
```

---

## Ontology

### Entity Types

| Type | ID Format | Description |
|------|-----------|-------------|
| `Issue` | `issue:<number>` | GitHub issue with title, state, body |
| `Person` | `github:<login>` | GitHub user (author, assignee, commenter) |
| `Component` | `component:<slug>` | VSCode subsystem (terminal, debugger, git…) |
| `Label` | `label:<name>` | GitHub label |
| `Claim` | `claim:<sha256[:16]>` | Extracted fact with evidence |
| `Evidence` | `evidence:<claim_id>:<idx>` | Source excerpt with offsets |

### Claim Types

| Claim Type | Description |
|------------|-------------|
| `ISSUE_ASSIGNED_TO` | Issue → Person |
| `ISSUE_AFFECTS_COMPONENT` | Issue → Component |
| `ISSUE_CLOSED_AS` | Resolution (fixed / wontfix / duplicate) |
| `ISSUE_DUPLICATE_OF` | Issue → Issue |
| `ISSUE_FIXED_BY` | Issue → PR |
| `DECISION_MADE` | Free-text decision from discussion |
| `BUG_REPORTED` | Bug in Component |
| `FEATURE_REQUESTED` | Feature request for Component |
| `REGRESSION_INTRODUCED` | Regression by commit/PR |
| `WORKAROUND_AVAILABLE` | Workaround described |
| `BREAKING_CHANGE` | Breaking change in Component |
| `PERSON_OWNS_COMPONENT` | Person → Component |

### Evidence Schema

Every claim carries:
```json
{
  "source_id":    "issue:12345",
  "excerpt":      "verbatim text ≤200 chars",
  "offset_start": 342,
  "offset_end":   489,
  "timestamp":    "2024-01-15T10:23:00Z",
  "url":          "https://github.com/microsoft/vscode/issues/12345"
}
```

---

## Deduplication Strategy

### 1. Artifact Dedup (Near-duplicate Issues)
- Token-level Jaccard similarity on `title + body[:500]`
- Threshold: 0.72 — lower number is kept as canonical
- Represented as `DUPLICATE_OF` edges in Neo4j

### 2. Entity Canonicalization
- **Persons:** `github:<lowercase_login>` — GitHub logins are already canonical
- **Components:** 14-way alias map (e.g. "monaco", "text editor", "code editor" → `component:editor`)
- Aliases stored on the node, used in full-text search

### 3. Claim Dedup
- Merge key: `claim_type + subject_id + object_id + value[:50]`
- On collision: keep highest confidence, union all evidence
- Full merge audit trail in `outputs/merge_audit_log.json`

### 4. Conflict Detection
- Same `claim_type + subject_id`, different values → flagged as conflict
- Both claims kept with different `valid_from` / `valid_until`
- Superseded claims marked with `superseded_by` pointing to newer claim

### 5. Reversibility
- All merges logged with `{action, merged_id, into_id, reason}`
- Merge log is append-only — full audit trail
- To undo: restore original claim IDs from `outputs/extractions/`

---

## Update Semantics

| Scenario | Handling |
|----------|----------|
| Issue state changes (open→closed) | MERGE updates `state`, `closed_at` on Issue node |
| Issue re-opened | MERGE updates state; old closure claim gets `valid_until` |
| Issue deleted | Set `valid_until` on all claims from that source |
| Assignee changes | New `ISSUE_ASSIGNED_TO` claim; old one gets `valid_until` |
| New comment adds evidence | New Evidence node linked to existing Claim |
| Re-extraction (schema change) | `extraction_version` bumped; old claims preserved |

**Idempotency:** All writes use Cypher `MERGE` — safe to re-run the pipeline.

---

## Neo4j Schema

```cypher
// Constraints
CONSTRAINT person_id    FOR (p:Person)    REQUIRE p.id IS UNIQUE
CONSTRAINT issue_id     FOR (i:Issue)     REQUIRE i.id IS UNIQUE
CONSTRAINT component_id FOR (c:Component) REQUIRE c.id IS UNIQUE
CONSTRAINT claim_id     FOR (c:Claim)     REQUIRE c.id IS UNIQUE

// Relationships
(Person)    -[:AUTHORED]->      (Issue)
(Issue)     -[:HAS_LABEL]->     (Label)
(Issue)     -[:DUPLICATE_OF]->  (Issue)
(Claim)     -[:HAS_SUBJECT]->   (Person|Component|Issue)
(Claim)     -[:HAS_OBJECT]->    (Person|Component|Issue)
(Claim)     -[:SUPPORTED_BY]->  (Evidence)
(Evidence)  -[:FROM_SOURCE]->   (Issue)
```

---

## Retrieval

For a question `q`:

1. **Embed** `q` with Gemini `text-embedding-004` (retrieval_query task type)
2. **Search Qdrant** — top-K cosine similarity over issues + claims
3. **Filter** — remove superseded claims, apply confidence threshold
4. **Expand** — fetch related entities from Neo4j for top issue hits
5. **Format** — return ranked results with evidence excerpts + citation refs

---

## Setup

### Prerequisites
- Python 3.11+
- Neo4j Aura free instance (already configured in `.env`)
- Qdrant cloud cluster (already configured in `.env`)

### Install
```bash
cd layer10
pip install -r requirements.txt
```

### Run full pipeline
```bash
python run_pipeline.py --max-issues 200
```

### Run with caching (skip already-done steps)
```bash
# After first run, skip fetch+extract, only rebuild graph
python run_pipeline.py --skip-fetch --skip-extract

# Skip everything, just run retrievals
python run_pipeline.py --skip-fetch --skip-extract --skip-graph --skip-vector
```

### Start API server
```bash
cd src
python api.py
# → http://localhost:5000
```

### Open visualization
```bash
open viz/index.html
# or serve it:
python -m http.server 8080 --directory viz
# → http://localhost:8080
```

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Node/edge counts |
| `GET /api/graph?limit=100&min_confidence=0.5` | Graph data for visualization |
| `GET /api/issue/<number>` | Issue detail with claims |
| `GET /api/entity/<id>` | Entity detail with claims |
| `GET /api/search?q=<question>&k=8` | Semantic retrieval context pack |
| `GET /api/merges` | Full merge audit log |
| `GET /api/conflicts` | Detected claim conflicts |
| `GET /api/duplicates` | Near-duplicate issue pairs |

---

## Adapting to Layer10's Target Environment

### Ontology Changes for Email + Slack + Jira/Linear

**New entity types:**
- `Thread` (email thread / Slack conversation)
- `Message` (individual message with sender, timestamp, channel)
- `Ticket` (Jira/Linear issue)
- `Project` / `Team` / `Customer`

**New claim types:**
- `DECISION_IN_THREAD` — decision made in email/Slack discussion
- `TICKET_MENTIONED_IN` — ticket referenced in chat
- `ACTION_ITEM_ASSIGNED` — action item assigned to person in meeting/thread
- `CUSTOMER_REPORTED` — customer report linked to ticket

**Fusion strategy:**
- `Thread` → `Ticket` links via issue number mentions (regex: `#\d+`, Linear IDs)
- `Message` evidence pointers carry `channel_id`, `message_ts` (Slack format)

### Long-Term Memory vs Ephemeral Context

| Durable Memory | Ephemeral Context |
|----------------|-------------------|
| Decisions with ≥2 supporting evidence | Single-message questions |
| Claims from closed tickets | Draft messages |
| Patterns repeated across 3+ sources | Short-lived status updates |
| Architecture decisions, ownership | Meeting small-talk |

**Decay policy:** Claims not re-evidenced within 90 days get `confidence *= 0.9` — eventually flagged for human review.

### Grounding & Safety (Deletions/Redactions)
- Every memory item stores `source_id` → if source is deleted, mark all derived claims as `valid_until = deletion_time`
- Redacted messages: replace excerpt with `[REDACTED]`, keep claim structure so the graph stays consistent
- Claims from redacted sources are excluded from retrieval by default (opt-in `include_redacted=True`)

### Permissions
- Each `Evidence` node stores `source_acl: [user_ids]` (inherited from Slack channel / Jira project membership)
- Retrieval query filters: `WHERE ALL(uid IN ev.source_acl WHERE uid = $current_user)` at Evidence level
- Claims with any non-accessible evidence are excluded from retrieval for that user

### Operational Reality
- **Incremental ingestion:** Webhook-driven (Slack Events API, Jira webhooks) → publish to queue → extraction worker
- **Cost:** Gemini Flash at ~$0.075/1M tokens → ~$0.002 per issue extraction at current size
- **Scaling:** Qdrant handles 100M+ vectors; Neo4j Aura scales to ~10M nodes on paid tiers
- **Regression testing:** Shadow-run new extraction versions on 100 held-out issues; compare claim counts and evidence coverage vs baseline

---

## Corpus Reproducibility

```bash
# The pipeline fetches automatically via GitHub API, but for manual download:
curl -H "Authorization: Bearer $GITHUB_TOKEN" \
  "https://api.github.com/repos/microsoft/vscode/issues?state=all&per_page=100&page=1" \
  > issues_page1.json
```

Issues are cached in `outputs/raw_issues/issue_<number>.json` — the pipeline is fully resumable.

---

## File Structure

```
layer10/
├── .env                          # credentials (git-ignored)
├── requirements.txt
├── run_pipeline.py               # main orchestrator
├── src/
│   ├── ontology.py               # schema, prompt, pydantic models
│   ├── fetcher.py                # GitHub API client
│   ├── extractor.py              # Gemini extraction
│   ├── dedup.py                  # dedup + canonicalization
│   ├── graph_builder.py          # Neo4j writer
│   ├── retrieval.py              # Qdrant indexing + search
│   └── api.py                    # Flask REST API
├── viz/
│   └── index.html                # interactive graph explorer
└── outputs/
    ├── raw_issues/               # cached GitHub JSON
    ├── extractions/              # Gemini extraction results
    ├── dedup/                    # canonicalized entities + claims
    ├── context_packs/            # example retrieval results
    ├── graph_stats.json
    └── merge_audit_log.json
```
