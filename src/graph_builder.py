"""
Neo4j Memory Graph Builder
Ingests entities, claims, and evidence into Neo4j Aura.
Idempotent MERGE operations — safe to re-run.
"""

import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


# ── Schema / Indexes ──────────────────────────────────────────────────────────

SCHEMA_QUERIES = [
    # Uniqueness constraints (also create indexes)
    "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT component_id IF NOT EXISTS FOR (c:Component) REQUIRE c.id IS UNIQUE",
    "CREATE CONSTRAINT issue_id IF NOT EXISTS FOR (i:Issue) REQUIRE i.id IS UNIQUE",
    "CREATE CONSTRAINT claim_id IF NOT EXISTS FOR (cl:Claim) REQUIRE cl.id IS UNIQUE",
    "CREATE CONSTRAINT label_id IF NOT EXISTS FOR (l:Label) REQUIRE l.id IS UNIQUE",

    # Full-text search indexes
    "CREATE FULLTEXT INDEX issue_text IF NOT EXISTS FOR (i:Issue) ON EACH [i.title, i.body_excerpt]",
    "CREATE FULLTEXT INDEX claim_text IF NOT EXISTS FOR (c:Claim) ON EACH [c.value, c.claim_type]",

    # Regular indexes for common lookups
    "CREATE INDEX issue_state IF NOT EXISTS FOR (i:Issue) ON (i.state)",
    "CREATE INDEX claim_type IF NOT EXISTS FOR (c:Claim) ON (c.claim_type)",
    "CREATE INDEX claim_confidence IF NOT EXISTS FOR (c:Claim) ON (c.confidence)",
]


# ── Neo4j Driver Context ──────────────────────────────────────────────────────

class MemoryGraphDB:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def close(self):
        self.driver.close()

    def run(self, query: str, params: dict = None):
        with self.driver.session(database=NEO4J_DATABASE) as session:
            return session.run(query, params or {}).data()

    def run_batch(self, query: str, records: list[dict], batch_size: int = 100):
        with self.driver.session(database=NEO4J_DATABASE) as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                session.run(query, {"batch": batch})

    def setup_schema(self):
        print("Setting up Neo4j schema …")
        for q in SCHEMA_QUERIES:
            try:
                self.run(q)
            except Exception as e:
                print(f"  Schema warning (ok to ignore): {e}")
        print("  Schema ready.")

    # ── Node Upserts ──────────────────────────────────────────────────────────

    def upsert_issues(self, issues: list[dict]):
        query = """
        UNWIND $batch AS row
        MERGE (i:Issue {id: row.id})
        SET i.number       = row.number,
            i.title        = row.title,
            i.state        = row.state,
            i.body_excerpt = row.body_excerpt,
            i.created_at   = row.created_at,
            i.updated_at   = row.updated_at,
            i.closed_at    = row.closed_at,
            i.url          = row.url,
            i.user         = row.user,
            i.updated_ts   = timestamp()
        """
        records = [{
            "id":           i["id"],
            "number":       i["number"],
            "title":        i["title"],
            "state":        i["state"],
            "body_excerpt": (i.get("body") or "")[:500],
            "created_at":   i["created_at"],
            "updated_at":   i["updated_at"],
            "closed_at":    i.get("closed_at", ""),
            "url":          i["html_url"],
            "user":         i["user"],
        } for i in issues]
        self.run_batch(query, records)
        print(f"  Upserted {len(records)} Issue nodes")

    def upsert_persons(self, persons: dict):
        query = """
        UNWIND $batch AS row
        MERGE (p:Person {id: row.id})
        SET p.login        = row.login,
            p.display_name = row.display_name,
            p.aliases      = row.aliases,
            p.updated_ts   = timestamp()
        """
        records = [
            {"id": p["id"], "login": p["login"],
             "display_name": p.get("display_name", ""),
             "aliases": p.get("aliases", [])}
            for p in persons.values()
        ]
        self.run_batch(query, records)
        print(f"  Upserted {len(records)} Person nodes")

    def upsert_components(self, components: dict):
        query = """
        UNWIND $batch AS row
        MERGE (c:Component {id: row.id})
        SET c.name        = row.name,
            c.aliases     = row.aliases,
            c.description = row.description,
            c.updated_ts  = timestamp()
        """
        records = [
            {"id": c["id"], "name": c["name"],
             "aliases": c.get("aliases", []),
             "description": c.get("description", "")}
            for c in components.values()
        ]
        self.run_batch(query, records)
        print(f"  Upserted {len(records)} Component nodes")

    def upsert_labels(self, issues: list[dict]):
        """Extract and upsert unique labels from issues."""
        seen = {}
        for issue in issues:
            for label in issue.get("labels", []):
                lid = f"label:{label.lower()}"
                if lid not in seen:
                    seen[lid] = {"id": lid, "name": label}

        if not seen:
            return

        query = """
        UNWIND $batch AS row
        MERGE (l:Label {id: row.id})
        SET l.name = row.name
        """
        self.run_batch(query, list(seen.values()))
        print(f"  Upserted {len(seen)} Label nodes")

    def upsert_claims(self, claims: list[dict]):
        """Upsert Claim nodes with all properties."""
        query = """
        UNWIND $batch AS row
        MERGE (cl:Claim {id: row.id})
        SET cl.claim_type          = row.claim_type,
            cl.subject_id          = row.subject_id,
            cl.object_id           = row.object_id,
            cl.value               = row.value,
            cl.confidence          = row.confidence,
            cl.valid_from          = row.valid_from,
            cl.valid_until         = row.valid_until,
            cl.superseded_by       = row.superseded_by,
            cl.extraction_version  = row.extraction_version,
            cl.evidence_count      = row.evidence_count,
            cl.updated_ts          = timestamp()
        """
        records = [{
            "id":                  c["id"],
            "claim_type":          c["claim_type"],
            "subject_id":          c["subject_id"],
            "object_id":           c.get("object_id", ""),
            "value":               c.get("value", ""),
            "confidence":          c.get("confidence", 0.7),
            "valid_from":          c.get("valid_from", ""),
            "valid_until":         c.get("valid_until", ""),
            "superseded_by":       c.get("superseded_by", ""),
            "extraction_version":  c.get("extraction_version", "v1"),
            "evidence_count":      len(c.get("evidence", [])),
        } for c in claims]
        self.run_batch(query, records)
        print(f"  Upserted {len(records)} Claim nodes")

    def upsert_evidence_nodes(self, claims: list[dict]):
        """Create Evidence nodes linked to Claims."""
        query = """
        UNWIND $batch AS row
        MERGE (e:Evidence {id: row.id})
        SET e.source_id     = row.source_id,
            e.excerpt       = row.excerpt,
            e.offset_start  = row.offset_start,
            e.offset_end    = row.offset_end,
            e.timestamp     = row.timestamp,
            e.url           = row.url
        WITH e, row
        MATCH (cl:Claim {id: row.claim_id})
        MERGE (cl)-[:SUPPORTED_BY]->(e)
        WITH e, row
        MATCH (i:Issue {id: row.source_id})
        MERGE (e)-[:FROM_SOURCE]->(i)
        """
        records = []
        for claim in claims:
            for idx, ev in enumerate(claim.get("evidence", [])):
                ev_id = f"evidence:{claim['id']}:{idx}"
                records.append({
                    "id":           ev_id,
                    "claim_id":     claim["id"],
                    "source_id":    ev["source_id"],
                    "excerpt":      ev["excerpt"],
                    "offset_start": ev.get("offset_start", 0),
                    "offset_end":   ev.get("offset_end", 0),
                    "timestamp":    ev.get("timestamp", ""),
                    "url":          ev.get("url", ""),
                })
        self.run_batch(query, records)
        print(f"  Upserted {len(records)} Evidence nodes")

    # ── Relationship Upserts ──────────────────────────────────────────────────

    def link_claims_to_entities(self, claims: list[dict],
                                 persons: dict, components: dict,
                                 issues: list[dict]):
        """Create SUBJECT/OBJECT edges between Claims and entity nodes."""
        issue_ids = {i["id"] for i in issues}

        subject_rels = []
        object_rels  = []

        for claim in claims:
            sid = claim["subject_id"]
            oid = claim.get("object_id", "")

            subject_rels.append({"claim_id": claim["id"], "entity_id": sid})
            if oid:
                object_rels.append({"claim_id": claim["id"], "entity_id": oid})

        # Subject links
        for label in ["Person", "Component", "Issue"]:
            q = f"""
            UNWIND $batch AS row
            MATCH (cl:Claim {{id: row.claim_id}})
            MATCH (e:{label} {{id: row.entity_id}})
            MERGE (cl)-[:HAS_SUBJECT]->(e)
            """
            try:
                self.run_batch(q, subject_rels)
            except Exception:
                pass  # entity may not exist for this label type

        # Generic subject/object using APOC-free approach
        q_subj = """
        UNWIND $batch AS row
        MATCH (cl:Claim {id: row.claim_id})
        OPTIONAL MATCH (p:Person    {id: row.entity_id})
        OPTIONAL MATCH (c:Component {id: row.entity_id})
        OPTIONAL MATCH (i:Issue     {id: row.entity_id})
        WITH cl, coalesce(p, c, i) AS entity
        WHERE entity IS NOT NULL
        MERGE (cl)-[:HAS_SUBJECT]->(entity)
        """
        q_obj = """
        UNWIND $batch AS row
        MATCH (cl:Claim {id: row.claim_id})
        OPTIONAL MATCH (p:Person    {id: row.entity_id})
        OPTIONAL MATCH (c:Component {id: row.entity_id})
        OPTIONAL MATCH (i:Issue     {id: row.entity_id})
        WITH cl, coalesce(p, c, i) AS entity
        WHERE entity IS NOT NULL
        MERGE (cl)-[:HAS_OBJECT]->(entity)
        """
        self.run_batch(q_subj, subject_rels)
        if object_rels:
            self.run_batch(q_obj, object_rels)
        print(f"  Linked {len(subject_rels)} subject + {len(object_rels)} object relationships")

    def link_issues_to_labels(self, issues: list[dict]):
        records = []
        for issue in issues:
            for label in issue.get("labels", []):
                records.append({
                    "issue_id": issue["id"],
                    "label_id": f"label:{label.lower()}",
                })
        if not records:
            return
        q = """
        UNWIND $batch AS row
        MATCH (i:Issue {id: row.issue_id})
        MATCH (l:Label {id: row.label_id})
        MERGE (i)-[:HAS_LABEL]->(l)
        """
        self.run_batch(q, records)
        print(f"  Linked {len(records)} Issue→Label relationships")

    def link_issue_authors(self, issues: list[dict]):
        records = [
            {"issue_id": i["id"], "person_id": f"github:{i['user'].lower()}"}
            for i in issues
        ]
        q = """
        UNWIND $batch AS row
        MATCH (i:Issue  {id: row.issue_id})
        MATCH (p:Person {id: row.person_id})
        MERGE (p)-[:AUTHORED]->(i)
        """
        self.run_batch(q, records)
        print(f"  Linked {len(records)} Person→Issue AUTHORED relationships")

    def link_duplicate_issues(self, dupes: dict):
        if not dupes:
            return
        records = [
            {"dup_id": dup, "canonical_id": canonical}
            for dup, canonical in dupes.items()
        ]
        q = """
        UNWIND $batch AS row
        MATCH (dup:Issue {id: row.dup_id})
        MATCH (can:Issue {id: row.canonical_id})
        MERGE (dup)-[:DUPLICATE_OF]->(can)
        """
        self.run_batch(q, records)
        print(f"  Linked {len(records)} DUPLICATE_OF relationships")

    # ── Summaries ─────────────────────────────────────────────────────────────

    def upsert_summaries(self, extractions: list[dict]):
        """Store LLM-generated summaries on Issue nodes."""
        records = [
            {"issue_id": e["issue_id"], "summary": e.get("summary", "")}
            for e in extractions if e.get("summary")
        ]
        q = """
        UNWIND $batch AS row
        MATCH (i:Issue {id: row.issue_id})
        SET i.summary = row.summary
        """
        self.run_batch(q, records)
        print(f"  Attached {len(records)} AI summaries to Issues")

    # ── Graph Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        stats = {}
        for label in ["Issue", "Person", "Component", "Label", "Claim", "Evidence"]:
            result = self.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            stats[label] = result[0]["cnt"] if result else 0
        rel_result = self.run("MATCH ()-[r]->() RETURN count(r) AS cnt")
        stats["Relationships"] = rel_result[0]["cnt"] if rel_result else 0
        return stats


# ── Main Build Function ───────────────────────────────────────────────────────

def build_memory_graph(
    issues: list[dict],
    dedup_result: dict,
    extractions: list[dict],
) -> MemoryGraphDB:
    print("\nBuilding Neo4j memory graph …")
    db = MemoryGraphDB()
    db.setup_schema()

    # Nodes
    db.upsert_issues(issues)
    db.upsert_persons(dedup_result["persons"])
    db.upsert_components(dedup_result["components"])
    db.upsert_labels(issues)
    db.upsert_claims(dedup_result["claims"])
    db.upsert_evidence_nodes(dedup_result["claims"])
    db.upsert_summaries(extractions)

    # Relationships
    db.link_issue_authors(issues)
    db.link_issues_to_labels(issues)
    db.link_claims_to_entities(
        dedup_result["claims"],
        dedup_result["persons"],
        dedup_result["components"],
        issues,
    )
    db.link_duplicate_issues(dedup_result["artifact_dupes"])

    stats = db.get_stats()
    print(f"\nGraph stats: {stats}")

    # Save stats
    Path("outputs/graph_stats.json").write_text(
        json.dumps(stats, indent=2)
    )

    return db


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    from fetcher import fetch_issues
    from extractor import run_extraction_pipeline
    from dedup import run_dedup_pipeline

    issues      = fetch_issues(max_issues=200)
    extractions = run_extraction_pipeline(issues)
    dedup       = run_dedup_pipeline(issues, extractions)
    db          = build_memory_graph(issues, dedup, extractions)
    db.close()
    print("Memory graph built successfully.")
