"""
Layer10 Main Pipeline
Runs the full end-to-end pipeline:
  1. Fetch GitHub issues
  2. Extract entities + claims (Gemini)
  3. Deduplicate + canonicalize
  4. Build Neo4j memory graph
  5. Build Qdrant vector store
  6. Run example retrievals
"""

import sys
import json
import time
from pathlib import Path

sys.path.insert(0, "src")

Path("outputs").mkdir(exist_ok=True)


def run_pipeline(max_issues: int = 200, skip_fetch: bool = False,
                 skip_extract: bool = False, skip_graph: bool = False,
                 skip_vector: bool = False):

    print("=" * 60)
    print("  LAYER10 MEMORY GRAPH PIPELINE")
    print("  Corpus: microsoft/vscode (GitHub Issues)")
    print("=" * 60)

    # ── Step 1: Fetch ─────────────────────────────────────────────────────────
    print("\n[1/5] FETCHING GITHUB ISSUES")
    print("-" * 40)

    if skip_fetch and Path("outputs/raw_issues").exists():
        from fetcher import fetch_issues
        # Just load from cache
        issues = []
        for f in sorted(Path("outputs/raw_issues").glob("*.json"))[:max_issues]:
            with open(f) as fp:
                issues.append(json.load(fp))
        print(f"  Loaded {len(issues)} issues from cache.")
    else:
        from fetcher import fetch_issues
        issues = fetch_issues(max_issues=max_issues)

    # Save manifest
    with open("outputs/issues_manifest.json", "w") as f:
        json.dump([{
            "id": i["id"], "number": i["number"],
            "title": i["title"], "state": i["state"],
            "created_at": i["created_at"],
        } for i in issues], f, indent=2)

    # ── Step 2: Extract ───────────────────────────────────────────────────────
    print("\n[2/5] STRUCTURED EXTRACTION (Gemini)")
    print("-" * 40)

    if skip_extract and Path("outputs/extractions").exists():
        extractions = []
        for f in sorted(Path("outputs/extractions").glob("*.json")):
            with open(f) as fp:
                extractions.append(json.load(fp))
        print(f"  Loaded {len(extractions)} extractions from cache.")
    else:
        from extractor import run_extraction_pipeline
        extractions = run_extraction_pipeline(issues)

    # ── Step 3: Dedup ─────────────────────────────────────────────────────────
    print("\n[3/5] DEDUPLICATION & CANONICALIZATION")
    print("-" * 40)
    from dedup import run_dedup_pipeline
    dedup_result = run_dedup_pipeline(issues, extractions)

    # ── Step 4: Build Neo4j Graph ─────────────────────────────────────────────
    print("\n[4/5] BUILDING NEO4J MEMORY GRAPH")
    print("-" * 40)

    if skip_graph:
        print("  Skipping graph build (skip_graph=True)")
        from graph_builder import MemoryGraphDB
        db = MemoryGraphDB()
    else:
        from graph_builder import build_memory_graph
        db = build_memory_graph(issues, dedup_result, extractions)

    # ── Step 5: Build Vector Store ────────────────────────────────────────────
    print("\n[5/5] BUILDING QDRANT VECTOR STORE")
    print("-" * 40)

    if skip_vector:
        print("  Skipping vector store build (skip_vector=True)")
        from retrieval import get_qdrant_client
        qdrant = get_qdrant_client()
    else:
        from retrieval import build_vector_store
        qdrant = build_vector_store(
            issues=issues,
            extractions=extractions,
            claims=dedup_result["claims"],
            recreate=True,
        )

    # ── Example Retrievals ────────────────────────────────────────────────────
    print("\n[+] RUNNING EXAMPLE RETRIEVALS")
    print("-" * 40)
    from retrieval import run_example_retrievals
    context_packs = run_example_retrievals(qdrant, db)

    db.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Issues fetched:     {len(issues)}")
    print(f"  Extractions:        {len(extractions)}")
    print(f"  Persons:            {dedup_result['stats']['total_persons']}")
    print(f"  Components:         {dedup_result['stats']['total_components']}")
    print(f"  Claims:             {dedup_result['stats']['total_claims']}")
    print(f"  Artifact dupes:     {dedup_result['stats']['artifact_dupes']}")
    print(f"  Conflicts:          {dedup_result['stats']['conflicts']}")
    print(f"  Context packs:      {len(context_packs)}")
    print()
    print("  Outputs:")
    print("    outputs/raw_issues/           — raw GitHub data")
    print("    outputs/extractions/          — Gemini extractions")
    print("    outputs/dedup/                — dedup results")
    print("    outputs/context_packs/        — example retrieval packs")
    print("    outputs/graph_stats.json      — Neo4j graph stats")
    print("    outputs/merge_audit_log.json  — merge audit trail")
    print()
    print("  Next steps:")
    print("    cd src && python api.py       — start API server (port 5000)")
    print("    open viz/index.html           — open visualization UI")
    print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Layer10 Pipeline")
    parser.add_argument("--max-issues",    type=int,  default=200)
    parser.add_argument("--skip-fetch",    action="store_true")
    parser.add_argument("--skip-extract",  action="store_true")
    parser.add_argument("--skip-graph",    action="store_true")
    parser.add_argument("--skip-vector",   action="store_true")
    args = parser.parse_args()

    run_pipeline(
        max_issues=args.max_issues,
        skip_fetch=args.skip_fetch,
        skip_extract=args.skip_extract,
        skip_graph=args.skip_graph,
        skip_vector=args.skip_vector,
    )
