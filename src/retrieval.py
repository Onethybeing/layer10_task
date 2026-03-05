"""
Qdrant Vector Store + Retrieval Engine
- Embeds issues/claims using Gemini embeddings
- Semantic search over memory
- Returns grounded context packs with citations
"""

import os
import json
import time
import hashlib
from pathlib import Path
from typing import Optional
import google.generativeai as genai
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue,
    SearchParams, ScoredPoint,
)
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

QDRANT_URL        = os.getenv("QDRANT_URL")
QDRANT_API_KEY    = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "layer10_vscode")
EMBEDDING_DIM     = 3072  # Gemini gemini-embedding-001 dimension


def get_qdrant_client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, timeout=60)


def embed_text(text: str) -> list[float]:
    """Embed a single text with Gemini gemini-embedding-001 via REST API."""
    import requests as _requests
    api_key = os.getenv("GEMINI_API_KEY")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={api_key}"
    payload = {
        "content": {"parts": [{"text": text[:8000]}]},
        "taskType": "RETRIEVAL_DOCUMENT",
    }
    r = _requests.post(url, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["embedding"]["values"]


def embed_query(text: str) -> list[float]:
    """Embed a query with Gemini gemini-embedding-001 via REST API."""
    import requests as _requests
    api_key = os.getenv("GEMINI_API_KEY")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={api_key}"
    payload = {
        "content": {"parts": [{"text": text}]},
        "taskType": "RETRIEVAL_QUERY",
    }
    r = _requests.post(url, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["embedding"]["values"]


# ── Collection Setup ──────────────────────────────────────────────────────────

def setup_collection(client: QdrantClient, recreate: bool = False):
    collections = [c.name for c in client.get_collections().collections]

    if QDRANT_COLLECTION in collections:
        if recreate:
            client.delete_collection(QDRANT_COLLECTION)
            print(f"  Deleted existing collection '{QDRANT_COLLECTION}'")
        else:
            print(f"  Collection '{QDRANT_COLLECTION}' already exists, skipping creation.")
            return

    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
    )
    print(f"  Created collection '{QDRANT_COLLECTION}' (dim={EMBEDDING_DIM})")


# ── Indexing ──────────────────────────────────────────────────────────────────

def index_issues(
    client: QdrantClient,
    issues: list[dict],
    extractions: list[dict],
    batch_size: int = 50,
):
    """Embed and index issues into Qdrant."""
    ext_map = {e["issue_id"]: e for e in extractions}
    points  = []

    print(f"Embedding {len(issues)} issues …")

    for issue in tqdm(issues):
        issue_id = issue["id"]
        ext = ext_map.get(issue_id, {})

        # Build rich text for embedding
        text_parts = [
            f"{issue['title']}",
            (issue.get("body") or "")[:1000],
            ext.get("summary", ""),
        ]
        # Add claim values for richer semantic representation
        for claim in ext.get("claims", [])[:5]:
            if claim.get("value"):
                text_parts.append(claim["value"])
        text = " ".join(filter(None, text_parts))[:4000]

        try:
            vector = embed_text(text)
        except Exception as e:
            print(f"  Embed error for {issue_id}: {e}")
            time.sleep(5)
            continue

        # Qdrant point id: use numeric hash
        point_id = abs(hash(issue_id)) % (2**31)

        payload = {
            "id":           issue_id,
            "type":         "issue",
            "number":       issue["number"],
            "title":        issue["title"],
            "state":        issue["state"],
            "url":          issue["html_url"],
            "created_at":   issue["created_at"],
            "user":         issue["user"],
            "labels":       issue.get("labels", []),
            "summary":      ext.get("summary", ""),
            "body_excerpt": (issue.get("body") or "")[:300],
        }

        points.append(PointStruct(id=point_id, vector=vector, payload=payload))

        # Rate limit for Gemini free tier
        time.sleep(0.5)

        # Flush batch
        if len(points) >= batch_size:
            client.upsert(collection_name=QDRANT_COLLECTION, points=points)
            points = []

    if points:
        client.upsert(collection_name=QDRANT_COLLECTION, points=points)

    print(f"  Indexed {len(issues)} issues into Qdrant")


def index_claims(
    client: QdrantClient,
    claims: list[dict],
    issues: list[dict],
    batch_size: int = 50,
):
    """Embed and index claims into Qdrant."""
    issue_map = {i["id"]: i for i in issues}
    points = []

    # Filter to high-confidence claims with values
    indexable = [
        c for c in claims
        if c.get("value") and c.get("confidence", 0) >= 0.6
    ]

    print(f"Embedding {len(indexable)} high-confidence claims …")

    for claim in tqdm(indexable):
        # Build claim text
        evidence_excerpts = " ".join(
            ev.get("excerpt", "") for ev in claim.get("evidence", [])[:2]
        )
        text = f"{claim['claim_type']}: {claim.get('value', '')} {evidence_excerpts}"

        try:
            vector = embed_text(text[:2000])
        except Exception as e:
            print(f"  Embed error for claim {claim['id']}: {e}")
            time.sleep(5)
            continue

        point_id = abs(hash(claim["id"])) % (2**31)

        # Get source issue URL
        source_issue = None
        for ev in claim.get("evidence", []):
            sid = ev.get("source_id", "")
            if sid in issue_map:
                source_issue = issue_map[sid]
                break

        payload = {
            "id":                 claim["id"],
            "type":               "claim",
            "claim_type":         claim["claim_type"],
            "subject_id":         claim["subject_id"],
            "object_id":          claim.get("object_id", ""),
            "value":              claim.get("value", ""),
            "confidence":         claim.get("confidence", 0.7),
            "valid_from":         claim.get("valid_from", ""),
            "valid_until":        claim.get("valid_until", ""),
            "superseded_by":      claim.get("superseded_by", ""),
            "evidence_count":     len(claim.get("evidence", [])),
            "source_url":         source_issue["html_url"] if source_issue else "",
            "evidence_excerpts":  [e.get("excerpt", "")[:200]
                                   for e in claim.get("evidence", [])[:3]],
        }

        points.append(PointStruct(id=point_id, vector=vector, payload=payload))
        time.sleep(0.5)

        if len(points) >= batch_size:
            client.upsert(collection_name=QDRANT_COLLECTION, points=points)
            points = []

    if points:
        client.upsert(collection_name=QDRANT_COLLECTION, points=points)

    print(f"  Indexed {len(indexable)} claims into Qdrant")


# ── Retrieval ─────────────────────────────────────────────────────────────────

def retrieve_context_pack(
    question: str,
    client: QdrantClient,
    db,  # MemoryGraphDB
    top_k: int = 10,
    min_confidence: float = 0.5,
    include_superseded: bool = False,
) -> dict:
    """
    Given a question, return a grounded context pack:
    {
      question, ranked_results, entities, claims, citations
    }
    """
    query_vec = embed_query(question)

    # Search Qdrant
    hits = client.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=query_vec,
        limit=top_k * 2,  # over-fetch, then filter
        with_payload=True,
        search_params=SearchParams(hnsw_ef=128),
    )

    # ── Filter and rank ───────────────────────────────────────────────────────
    results = []
    seen_ids = set()

    for hit in hits:
        p = hit.payload
        item_id = p.get("id", "")

        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        # Skip superseded claims
        if p.get("type") == "claim":
            if p.get("superseded_by") and not include_superseded:
                continue
            if p.get("confidence", 1.0) < min_confidence:
                continue

        results.append({
            "score":        round(hit.score, 4),
            "type":         p.get("type", "unknown"),
            "id":           item_id,
            "title":        p.get("title", p.get("value", "")),
            "url":          p.get("url", p.get("source_url", "")),
            "state":        p.get("state", ""),
            "claim_type":   p.get("claim_type", ""),
            "subject_id":   p.get("subject_id", ""),
            "object_id":    p.get("object_id", ""),
            "value":        p.get("value", ""),
            "confidence":   p.get("confidence", 1.0),
            "valid_from":   p.get("valid_from", ""),
            "valid_until":  p.get("valid_until", ""),
            "body_excerpt": p.get("body_excerpt", ""),
            "summary":      p.get("summary", ""),
            "evidence_excerpts": p.get("evidence_excerpts", []),
            "labels":       p.get("labels", []),
        })

        if len(results) >= top_k:
            break

    # ── Pull related entities from Neo4j ─────────────────────────────────────
    related_entities = []
    issue_ids = [r["id"] for r in results if r["type"] == "issue"]

    if issue_ids and db:
        try:
            neo_results = db.run("""
            UNWIND $ids AS iid
            MATCH (i:Issue {id: iid})
            OPTIONAL MATCH (p:Person)-[:AUTHORED]->(i)
            OPTIONAL MATCH (i)-[:HAS_LABEL]->(l:Label)
            OPTIONAL MATCH (cl:Claim)-[:HAS_SUBJECT]->(i)
            RETURN i.id AS issue_id,
                   collect(DISTINCT p.login) AS authors,
                   collect(DISTINCT l.name) AS labels,
                   count(DISTINCT cl) AS claim_count
            LIMIT 10
            """, {"ids": issue_ids[:5]})
            related_entities = neo_results
        except Exception as e:
            print(f"  Neo4j query warning: {e}")

    # ── Build citations ───────────────────────────────────────────────────────
    citations = []
    for idx, r in enumerate(results):
        if r["url"]:
            citations.append({
                "ref":     f"[{idx+1}]",
                "id":      r["id"],
                "url":     r["url"],
                "title":   r["title"][:100],
                "type":    r["type"],
                "score":   r["score"],
            })

    return {
        "question":        question,
        "ranked_results":  results,
        "related_entities": related_entities,
        "citations":       citations,
        "total_found":     len(results),
    }


# ── Batch Example Retrieval ───────────────────────────────────────────────────

EXAMPLE_QUESTIONS = [
    "What are the most common bugs in the VS Code terminal?",
    "Which issues are related to extension host crashes?",
    "What decisions were made about the debugger?",
    "Who are the main contributors working on git integration?",
    "What performance issues have been reported?",
    "Are there any breaking changes in the editor component?",
    "What workarounds exist for known issues?",
]


def run_example_retrievals(client: QdrantClient, db) -> list[dict]:
    """Run all example questions and save context packs."""
    results = []
    out_dir = Path("outputs/context_packs")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\nRunning example retrievals …")

    for q in EXAMPLE_QUESTIONS:
        print(f"  Q: {q}")
        pack = retrieve_context_pack(q, client, db, top_k=5)
        results.append(pack)
        print(f"    → {pack['total_found']} results")

        safe_name = q[:40].replace(" ", "_").replace("?", "").replace("/", "_")
        with open(out_dir / f"{safe_name}.json", "w") as f:
            json.dump(pack, f, indent=2)

        time.sleep(1)  # rate limit

    print(f"\nSaved {len(results)} context packs to {out_dir}/")
    return results


# ── Build Vector Store ────────────────────────────────────────────────────────

def build_vector_store(
    issues: list[dict],
    extractions: list[dict],
    claims: list[dict],
    recreate: bool = False,
) -> QdrantClient:
    client = get_qdrant_client()
    setup_collection(client, recreate=recreate)
    index_issues(client, issues, extractions)
    index_claims(client, claims, issues)
    print("\nVector store built.")
    return client
