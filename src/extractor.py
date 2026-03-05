"""
Structured Extraction Pipeline using Gemini
Extracts entities + claims from GitHub issues with evidence grounding.
BATCHED: sends 10 issues per API call to minimize quota usage.
"""

import os
import json
import hashlib
import time
from pathlib import Path
from typing import Optional
import google.generativeai as genai
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

from ontology import CLAIM_TYPES, Evidence, Claim

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

EXTRACTION_DIR = Path("outputs/extractions")
EXTRACTION_VERSION = "v1"
MODEL_NAME = "gemini-2.5-flash"  # free tier
BATCH_SIZE = 20  # issues per API call


# ── Batched extraction prompt ─────────────────────────────────────────────────

BATCH_EXTRACTION_PROMPT = """You are an expert knowledge extractor for a software engineering memory system.

Given MULTIPLE GitHub issues (with comments) below, extract structured information for EACH issue.

Return ONLY valid JSON — an array with one object per issue, matching this schema:

[
  {{
    "issue_number": <int>,
    "entities": {{
      "persons": [{{"login": "string", "display_name": "string or null"}}],
      "components": [{{"name": "string", "description": "string or null"}}],
      "labels": [{{"name": "string"}}]
    }},
    "claims": [
      {{
        "claim_type": "one of: {claim_types}",
        "subject_ref": "login or component_name or issue_number as string",
        "object_ref": "login or component_name or issue_number as string or null",
        "value": "string description or null",
        "confidence": 0.0-1.0,
        "evidence_excerpt": "exact quote from text ≤200 chars",
        "evidence_offset_start": 0,
        "evidence_offset_end": 0
      }}
    ],
    "summary": "1-2 sentence summary of the issue"
  }}
]

Rules:
- Return one JSON object per issue in the array, keyed by issue_number
- Only extract claims clearly supported by the text
- confidence > 0.8 for explicit statements, 0.5-0.8 for inferred
- evidence_excerpt must be verbatim from the input text
- For DECISION_MADE: value should describe the decision clearly
- Components are vscode subsystems: editor, terminal, debugger, extensions, git, search, themes, etc.

Issues to extract from:
---
{issues_text}
---"""


def make_claim_id(issue_id: str, claim_type: str, subject: str, obj: str) -> str:
    raw = f"{issue_id}|{claim_type}|{subject}|{obj or ''}"
    return "claim:" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def find_excerpt_offset(full_text: str, excerpt: str) -> tuple[int, int]:
    """Find char offsets of excerpt in full_text. Returns (-1,-1) if not found."""
    if not excerpt:
        return (-1, -1)
    idx = full_text.find(excerpt[:80])
    if idx == -1:
        return (-1, -1)
    return (idx, idx + len(excerpt))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=4, min=15, max=120))
def call_gemini(prompt: str) -> str:
    """Call Gemini via REST API directly (bypasses library v1beta quota issues)."""
    import requests as _requests
    api_key = os.getenv("GEMINI_API_KEY")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }
    r = _requests.post(url, json=payload, timeout=120)
    if r.status_code == 429:
        raise Exception(f"Rate limited (429): {r.text[:200]}")
    r.raise_for_status()
    data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def build_claims_for_issue(issue: dict, issue_text: str, raw_claims: list) -> list[dict]:
    """Build grounded Claim objects from raw extracted claims."""
    built_claims = []
    for c in raw_claims:
        excerpt = c.get("evidence_excerpt", "")
        offset_start, offset_end = find_excerpt_offset(issue_text, excerpt)

        if offset_start == -1:
            offset_start = c.get("evidence_offset_start", 0)
            offset_end = c.get("evidence_offset_end", len(excerpt))

        claim_id = make_claim_id(
            issue["id"],
            c.get("claim_type", ""),
            c.get("subject_ref", ""),
            c.get("object_ref", ""),
        )

        evidence = Evidence(
            source_id=issue["id"],
            excerpt=excerpt,
            offset_start=offset_start,
            offset_end=offset_end,
            timestamp=issue["created_at"],
            url=issue["html_url"],
        )

        claim = Claim(
            id=claim_id,
            claim_type=c.get("claim_type", "DECISION_MADE"),
            subject_id=c.get("subject_ref", ""),
            object_id=c.get("object_ref"),
            value=c.get("value"),
            confidence=float(c.get("confidence", 0.7)),
            valid_from=issue["created_at"],
            valid_until=issue.get("closed_at"),
            evidence=[evidence],
            extraction_version=EXTRACTION_VERSION,
        )
        built_claims.append(claim.model_dump())
    return built_claims


def extract_batch(issues_batch: list[dict], issue_texts: dict[int, str]) -> list[dict]:
    """
    Extract entities + claims from a batch of issues in a SINGLE Gemini call.
    Returns list of extraction results.
    """
    EXTRACTION_DIR.mkdir(parents=True, exist_ok=True)

    # Check cache — only send uncached issues
    cached_results = []
    uncached_issues = []
    for issue in issues_batch:
        cache_path = EXTRACTION_DIR / f"extracted_{issue['number']}.json"
        if cache_path.exists():
            with open(cache_path) as f:
                cached_results.append(json.load(f))
        else:
            uncached_issues.append(issue)

    if not uncached_issues:
        return cached_results

    # Build combined prompt with all uncached issues
    separator = "\n\n" + "=" * 60 + "\n\n"
    combined_texts = []
    for issue in uncached_issues:
        text = issue_texts[issue["number"]]
        # Cap each issue to ~3000 chars to fit 10 in context
        combined_texts.append(text[:3000])

    issues_text = separator.join(combined_texts)
    claim_types_str = ", ".join(CLAIM_TYPES.keys())
    prompt = BATCH_EXTRACTION_PROMPT.format(
        claim_types=claim_types_str,
        issues_text=issues_text,
    )

    try:
        raw = call_gemini(prompt)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]

        batch_data = json.loads(raw)

        # Handle both array and single-object responses
        if isinstance(batch_data, dict):
            batch_data = [batch_data]

        # Index by issue_number
        data_by_number = {}
        for item in batch_data:
            num = item.get("issue_number")
            if num is not None:
                data_by_number[int(num)] = item

        # Process each uncached issue
        new_results = []
        for issue in uncached_issues:
            data = data_by_number.get(issue["number"], {})
            issue_text = issue_texts[issue["number"]]
            built_claims = build_claims_for_issue(issue, issue_text, data.get("claims", []))

            result = {
                "issue_id": issue["id"],
                "issue_number": issue["number"],
                "extraction_version": EXTRACTION_VERSION,
                "model": MODEL_NAME,
                "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "entities": data.get("entities", {}),
                "claims": built_claims,
                "summary": data.get("summary", ""),
                "raw_issue_text": issue_text,
            }

            cache_path = EXTRACTION_DIR / f"extracted_{issue['number']}.json"
            with open(cache_path, "w") as f:
                json.dump(result, f, indent=2)

            new_results.append(result)

        return cached_results + new_results

    except json.JSONDecodeError as e:
        numbers = [i["number"] for i in uncached_issues]
        print(f"  ⚠ JSON parse error for batch {numbers}: {e}")
        return cached_results
    except Exception as e:
        numbers = [i["number"] for i in uncached_issues]
        print(f"  ⚠ Extraction failed for batch {numbers}: {e}")
        return cached_results


# Legacy single-issue extraction (kept for compatibility)
def extract_issue(issue: dict, issue_text: str) -> Optional[dict]:
    results = extract_batch([issue], {issue["number"]: issue_text})
    return results[0] if results else None


def run_extraction_pipeline(issues: list[dict]) -> list[dict]:
    """
    Run BATCHED extraction — 10 issues per API call.
    50 issues = 5 API calls instead of 50.
    """
    from fetcher import build_issue_text

    results = []
    failed = []

    # Pre-build all issue texts
    issue_texts = {}
    for issue in issues:
        issue_texts[issue["number"]] = build_issue_text(issue)

    # Process in batches of BATCH_SIZE
    total_batches = (len(issues) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nRunning extraction on {len(issues)} issues in {total_batches} batches of {BATCH_SIZE} …")

    for batch_idx in range(0, len(issues), BATCH_SIZE):
        batch = issues[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        numbers = [i["number"] for i in batch]
        print(f"\n  Batch {batch_num}/{total_batches}: issues {numbers}")

        batch_results = extract_batch(batch, issue_texts)
        results.extend(batch_results)

        missing = set(i["number"] for i in batch) - set(r["issue_number"] for r in batch_results)
        if missing:
            failed.extend(missing)
            print(f"    ⚠ Missing from results: {list(missing)}")

        print(f"    ✓ Got {len(batch_results)} extractions")

        # Rate limit between batches
        if batch_idx + BATCH_SIZE < len(issues):
            print(f"    Waiting 10s before next batch …")
            time.sleep(10)

    print(f"\nExtraction complete: {len(results)} success, {len(failed)} failed")
    if failed:
        print(f"Failed issues: {failed}")

    # Save summary
    summary_path = Path("outputs/extraction_summary.json")
    with open(summary_path, "w") as f:
        json.dump({
            "total": len(issues),
            "success": len(results),
            "failed": failed,
            "extraction_version": EXTRACTION_VERSION,
            "model": MODEL_NAME,
        }, f, indent=2)

    return results


if __name__ == "__main__":
    from fetcher import fetch_issues
    issues = fetch_issues(max_issues=200)
    extractions = run_extraction_pipeline(issues)
    print(f"Done. {len(extractions)} extractions saved.")
