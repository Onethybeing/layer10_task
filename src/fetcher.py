"""
GitHub Issues Fetcher for microsoft/vscode
Fetches issues + comments with rate-limit handling.
"""

import os
import json
import time
import requests
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO  = os.getenv("GITHUB_REPO", "microsoft/vscode")
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
BASE_URL = "https://api.github.com"
RAW_DIR  = Path("outputs/raw_issues")


def gh_get(url: str, params: dict = None) -> dict | list:
    """GET with retry on rate-limit."""
    for attempt in range(5):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 403:
            reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait  = max(reset - int(time.time()), 1) + 5
            print(f"  Rate limited. Sleeping {wait}s …")
            time.sleep(wait)
        elif r.status_code == 404:
            return None
        else:
            print(f"  HTTP {r.status_code} on {url}, retrying …")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url}")


def fetch_issues(max_issues: int = 200, state: str = "all") -> list[dict]:
    """
    Fetch up to max_issues from the repo with their comments.
    Saves raw JSON to outputs/raw_issues/.
    Returns list of enriched issue dicts.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    issues_out = []
    page = 1
    per_page = 30
    fetched = 0

    print(f"Fetching issues from {GITHUB_REPO} …")

    while fetched < max_issues:
        batch = gh_get(
            f"{BASE_URL}/repos/{GITHUB_REPO}/issues",
            params={"state": state, "per_page": per_page, "page": page,
                    "sort": "updated", "direction": "desc"},
        )
        if not batch:
            break

        # Filter out pull requests (GitHub issues API returns PRs too)
        issues = [i for i in batch if "pull_request" not in i]

        for issue in tqdm(issues, desc=f"Page {page}", leave=False):
            if fetched >= max_issues:
                break

            number = issue["number"]
            cache_path = RAW_DIR / f"issue_{number}.json"

            if cache_path.exists():
                with open(cache_path) as f:
                    enriched = json.load(f)
            else:
                # Fetch comments
                comments_url = issue.get("comments_url", "")
                comments = []
                if issue.get("comments", 0) > 0 and comments_url:
                    raw_comments = gh_get(comments_url, params={"per_page": 50})
                    if raw_comments:
                        comments = [
                            {
                                "id": c["id"],
                                "user": c["user"]["login"] if c.get("user") else "ghost",
                                "body": c.get("body", ""),
                                "created_at": c["created_at"],
                                "updated_at": c["updated_at"],
                                "html_url": c["html_url"],
                            }
                            for c in raw_comments
                        ]

                enriched = {
                    "id": f"issue:{number}",
                    "number": number,
                    "title": issue.get("title", ""),
                    "state": issue.get("state", "open"),
                    "body": issue.get("body", "") or "",
                    "user": issue["user"]["login"] if issue.get("user") else "ghost",
                    "assignees": [a["login"] for a in issue.get("assignees", [])],
                    "labels": [l["name"] for l in issue.get("labels", [])],
                    "milestone": (issue["milestone"]["title"]
                                  if issue.get("milestone") else None),
                    "created_at": issue["created_at"],
                    "updated_at": issue["updated_at"],
                    "closed_at": issue.get("closed_at"),
                    "html_url": issue["html_url"],
                    "comments": comments,
                    "reactions": issue.get("reactions", {}),
                }

                with open(cache_path, "w") as f:
                    json.dump(enriched, f, indent=2)

            issues_out.append(enriched)
            fetched += 1

        if len(batch) < per_page:
            break
        page += 1

    print(f"Fetched {len(issues_out)} issues (with comments).")
    return issues_out


def build_issue_text(issue: dict) -> str:
    """
    Flatten an issue + comments into a single text block for LLM extraction.
    Returns (text, offset_map) where offset_map tracks segment boundaries.
    """
    parts = []
    parts.append(f"ISSUE #{issue['number']}: {issue['title']}")
    parts.append(f"State: {issue['state']}")
    parts.append(f"Author: {issue['user']}")
    if issue.get("assignees"):
        parts.append(f"Assignees: {', '.join(issue['assignees'])}")
    if issue.get("labels"):
        parts.append(f"Labels: {', '.join(issue['labels'])}")
    if issue.get("milestone"):
        parts.append(f"Milestone: {issue['milestone']}")
    parts.append(f"Created: {issue['created_at']}")
    if issue.get("closed_at"):
        parts.append(f"Closed: {issue['closed_at']}")
    parts.append("")
    parts.append("--- Description ---")
    body = (issue.get("body") or "")[:2000]  # cap at 2000 chars
    parts.append(body)

    for i, comment in enumerate(issue.get("comments", [])[:10]):  # max 10 comments
        parts.append(f"\n--- Comment {i+1} by {comment['user']} ({comment['created_at']}) ---")
        comment_body = (comment.get("body") or "")[:500]
        parts.append(comment_body)

    return "\n".join(parts)


if __name__ == "__main__":
    issues = fetch_issues(max_issues=200)
    print(f"Done. {len(issues)} issues saved to {RAW_DIR}/")
