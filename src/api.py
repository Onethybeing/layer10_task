"""
Layer10 REST API
Serves graph data and retrieval results to the visualization UI.
"""

import os
import json
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

# Ensure working directory is project root (not src/)
import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

app = Flask(__name__)
CORS(app)

# Lazy-loaded globals
_db = None
_qdrant = None


def get_db():
    global _db
    if _db is None:
        from graph_builder import MemoryGraphDB
        _db = MemoryGraphDB()
    return _db


def get_qdrant():
    global _qdrant
    if _qdrant is None:
        from retrieval import get_qdrant_client
        _qdrant = get_qdrant_client()
    return _qdrant


# ── Health ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({
        "service": "Layer10 Memory Graph API",
        "status": "running",
        "endpoints": {
            "/health": "Health check",
            "/api/stats": "Graph statistics",
            "/api/graph": "Graph nodes + edges for visualization",
            "/api/graph?limit=100&min_confidence=0.5": "Graph with params",
            "/api/entity/<id>": "Entity detail with claims",
            "/api/issue/<number>": "Issue detail",
            "/api/search?q=<query>&k=8": "Semantic search",
            "/api/merges": "Merge audit log",
            "/api/conflicts": "Claim conflicts",
            "/api/duplicates": "Duplicate issues",
        }
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── Graph Stats ───────────────────────────────────────────────────────────────

@app.route("/api/stats")
def stats():
    try:
        stats_path = Path("outputs/graph_stats.json")
        if stats_path.exists():
            return jsonify(json.loads(stats_path.read_text()))
        return jsonify(get_db().get_stats())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Graph Data for Visualization ──────────────────────────────────────────────

@app.route("/api/graph")
def graph_data():
    """Returns nodes + edges for the graph visualizer."""
    limit = int(request.args.get("limit", 100))
    node_type = request.args.get("type", "all")
    min_confidence = float(request.args.get("min_confidence", 0.5))

    try:
        db = get_db()

        # Fetch issues
        issues = db.run("""
        MATCH (i:Issue)
        RETURN i.id AS id, i.title AS title, i.state AS state,
               i.url AS url, i.created_at AS created_at,
               i.summary AS summary, i.number AS number
        LIMIT $limit
        """, {"limit": limit})

        # Fetch persons
        persons = db.run("""
        MATCH (p:Person)
        RETURN p.id AS id, p.login AS login, p.display_name AS display_name
        LIMIT 100
        """)

        # Fetch components
        components = db.run("""
        MATCH (c:Component)
        RETURN c.id AS id, c.name AS name, c.description AS description,
               c.aliases AS aliases
        LIMIT 100
        """)

        # Fetch claims
        claims = db.run("""
        MATCH (cl:Claim)
        WHERE cl.confidence >= $min_conf
          AND (cl.superseded_by IS NULL OR cl.superseded_by = '')
        RETURN cl.id AS id, cl.claim_type AS claim_type,
               cl.subject_id AS subject_id, cl.object_id AS object_id,
               cl.value AS value, cl.confidence AS confidence,
               cl.valid_from AS valid_from, cl.valid_until AS valid_until,
               cl.evidence_count AS evidence_count
        LIMIT 300
        """, {"min_conf": min_confidence})

        # Fetch relationships
        rels = db.run("""
        MATCH (a)-[r]->(b)
        WHERE type(r) IN ['AUTHORED', 'HAS_LABEL', 'DUPLICATE_OF', 'HAS_SUBJECT', 'HAS_OBJECT']
        RETURN id(a) AS source_neo_id, a.id AS source_id,
               type(r) AS rel_type,
               id(b) AS target_neo_id, b.id AS target_id
        LIMIT 500
        """)

        # Build nodes list
        nodes = []
        node_ids = set()

        for i in issues:
            if i["id"] not in node_ids:
                nodes.append({"id": i["id"], "type": "Issue", **i})
                node_ids.add(i["id"])

        for p in persons:
            if p["id"] not in node_ids:
                nodes.append({"id": p["id"], "type": "Person", **p})
                node_ids.add(p["id"])

        for c in components:
            if c["id"] not in node_ids:
                nodes.append({"id": c["id"], "type": "Component", **c})
                node_ids.add(c["id"])

        for cl in claims:
            if cl["id"] not in node_ids:
                nodes.append({"id": cl["id"], "type": "Claim", **cl})
                node_ids.add(cl["id"])

        # Build edges
        edges = []
        for r in rels:
            if r["source_id"] and r["target_id"]:
                edges.append({
                    "source": r["source_id"],
                    "target": r["target_id"],
                    "type":   r["rel_type"],
                })

        return jsonify({
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "issues":     len(issues),
                "persons":    len(persons),
                "components": len(components),
                "claims":     len(claims),
                "edges":      len(edges),
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Entity Detail ─────────────────────────────────────────────────────────────

@app.route("/api/entity/<path:entity_id>")
def entity_detail(entity_id):
    """Get full details for any entity including its claims and evidence."""
    try:
        db = get_db()

        # Try each node type
        result = db.run("""
        OPTIONAL MATCH (i:Issue     {id: $id})
        OPTIONAL MATCH (p:Person    {id: $id})
        OPTIONAL MATCH (c:Component {id: $id})
        OPTIONAL MATCH (cl:Claim    {id: $id})
        RETURN i, p, c, cl
        """, {"id": entity_id})

        if not result:
            return jsonify({"error": "Entity not found"}), 404

        row = result[0]

        # Get claims about this entity
        claims = db.run("""
        MATCH (cl:Claim)-[:HAS_SUBJECT|HAS_OBJECT]->(e {id: $id})
        OPTIONAL MATCH (cl)-[:SUPPORTED_BY]->(ev:Evidence)
        RETURN cl.id AS claim_id, cl.claim_type AS claim_type,
               cl.value AS value, cl.confidence AS confidence,
               cl.valid_from AS valid_from, cl.valid_until AS valid_until,
               cl.superseded_by AS superseded_by,
               collect({
                 excerpt: ev.excerpt,
                 url: ev.url,
                 timestamp: ev.timestamp,
                 source_id: ev.source_id
               }) AS evidence
        LIMIT 50
        """, {"id": entity_id})

        return jsonify({
            "entity_id": entity_id,
            "claims":    claims,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Issue Detail ──────────────────────────────────────────────────────────────

@app.route("/api/issue/<int:number>")
def issue_detail(number):
    try:
        db = get_db()
        issue_id = f"issue:{number}"

        issue = db.run("""
        MATCH (i:Issue {id: $id})
        OPTIONAL MATCH (p:Person)-[:AUTHORED]->(i)
        OPTIONAL MATCH (i)-[:HAS_LABEL]->(l:Label)
        OPTIONAL MATCH (i)-[:DUPLICATE_OF]->(dup:Issue)
        RETURN i.id AS id, i.title AS title, i.state AS state,
               i.body_excerpt AS body_excerpt, i.url AS url,
               i.created_at AS created_at, i.closed_at AS closed_at,
               i.summary AS summary,
               collect(DISTINCT p.login) AS authors,
               collect(DISTINCT l.name) AS labels,
               dup.id AS duplicate_of
        """, {"id": issue_id})

        if not issue:
            return jsonify({"error": "Issue not found"}), 404

        # Get claims for this issue
        claims = db.run("""
        MATCH (cl:Claim)-[:SUPPORTED_BY]->(ev:Evidence)-[:FROM_SOURCE]->(i:Issue {id: $id})
        OPTIONAL MATCH (cl)-[:SUPPORTED_BY]->(ev2:Evidence)
        RETURN cl.id AS claim_id, cl.claim_type AS claim_type,
               cl.value AS value, cl.confidence AS confidence,
               cl.subject_id AS subject_id, cl.object_id AS object_id,
               cl.valid_from AS valid_from, cl.superseded_by AS superseded_by,
               collect({excerpt: ev2.excerpt, url: ev2.url}) AS evidence
        LIMIT 20
        """, {"id": issue_id})

        return jsonify({
            "issue":  issue[0],
            "claims": claims,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Retrieval / Search ────────────────────────────────────────────────────────

@app.route("/api/search")
def search():
    """Semantic search returning a grounded context pack."""
    question = request.args.get("q", "").strip()
    top_k    = int(request.args.get("k", 8))

    if not question:
        return jsonify({"error": "Missing query parameter 'q'"}), 400

    try:
        from retrieval import retrieve_context_pack
        pack = retrieve_context_pack(
            question=question,
            client=get_qdrant(),
            db=get_db(),
            top_k=top_k,
        )
        return jsonify(pack)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Merge Audit Log ───────────────────────────────────────────────────────────

@app.route("/api/merges")
def merge_log():
    try:
        log_path = Path("outputs/merge_audit_log.json")
        if log_path.exists():
            return jsonify(json.loads(log_path.read_text()))
        return jsonify([])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Conflicts ─────────────────────────────────────────────────────────────────

@app.route("/api/conflicts")
def conflicts():
    try:
        path = Path("outputs/dedup/conflicts.json")
        if path.exists():
            return jsonify(json.loads(path.read_text()))
        return jsonify([])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Duplicate Issues ──────────────────────────────────────────────────────────

@app.route("/api/duplicates")
def duplicates():
    try:
        path = Path("outputs/dedup/artifact_dupes.json")
        if path.exists():
            return jsonify(json.loads(path.read_text()))
        return jsonify({})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
