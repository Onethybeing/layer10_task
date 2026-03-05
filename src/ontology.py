"""
Layer10 Ontology for GitHub Issues (microsoft/vscode)
Defines all entity types, claim types, and evidence schema.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime


# ── Entity Types ──────────────────────────────────────────────────────────────

class Person(BaseModel):
    id: str                          # canonical: "github:<login>"
    type: Literal["Person"] = "Person"
    login: str
    display_name: Optional[str] = None
    email: Optional[str] = None
    aliases: List[str] = Field(default_factory=list)

class Component(BaseModel):
    id: str                          # canonical: "component:<slug>"
    type: Literal["Component"] = "Component"
    name: str
    aliases: List[str] = Field(default_factory=list)
    description: Optional[str] = None

class Label(BaseModel):
    id: str                          # canonical: "label:<name>"
    type: Literal["Label"] = "Label"
    name: str
    color: Optional[str] = None
    description: Optional[str] = None

class Issue(BaseModel):
    id: str                          # canonical: "issue:<number>"
    type: Literal["Issue"] = "Issue"
    number: int
    title: str
    state: Literal["open", "closed"]
    created_at: str
    updated_at: str
    closed_at: Optional[str] = None
    url: str
    body_excerpt: Optional[str] = None   # first 500 chars

class PullRequest(BaseModel):
    id: str
    type: Literal["PullRequest"] = "PullRequest"
    number: int
    title: str
    state: str
    merged: bool
    created_at: str
    updated_at: str
    url: str

class Milestone(BaseModel):
    id: str
    type: Literal["Milestone"] = "Milestone"
    title: str
    state: str
    due_on: Optional[str] = None


# ── Claim Types (extracted by LLM) ───────────────────────────────────────────

class Evidence(BaseModel):
    source_id: str          # e.g. "issue:1234" or "comment:issue:1234:0"
    excerpt: str            # exact text snippet (≤300 chars)
    offset_start: int       # char offset in original text
    offset_end: int
    timestamp: str          # ISO8601
    url: str                # direct link to source

class Claim(BaseModel):
    id: str                             # deterministic hash
    claim_type: str                     # see CLAIM_TYPES below
    subject_id: str                     # entity id
    object_id: Optional[str] = None    # entity id (for relations)
    value: Optional[str] = None        # scalar value if not a relation
    confidence: float = Field(ge=0.0, le=1.0)
    valid_from: str                     # ISO8601
    valid_until: Optional[str] = None  # None = currently true
    superseded_by: Optional[str] = None
    evidence: List[Evidence] = Field(default_factory=list)
    extraction_version: str = "v1"


# ── Claim Type Registry ───────────────────────────────────────────────────────

CLAIM_TYPES = {
    # Issue lifecycle
    "ISSUE_ASSIGNED_TO":       "Issue assigned to Person",
    "ISSUE_LABELED":           "Issue has Label",
    "ISSUE_AFFECTS_COMPONENT": "Issue affects Component",
    "ISSUE_CLOSED_AS":         "Issue closed with resolution (fixed/wontfix/duplicate)",
    "ISSUE_DUPLICATE_OF":      "Issue is duplicate of another Issue",
    "ISSUE_BLOCKED_BY":        "Issue is blocked by another Issue",
    "ISSUE_FIXED_BY":          "Issue fixed by PullRequest",
    "ISSUE_MILESTONE":         "Issue belongs to Milestone",

    # Decisions / facts extracted from text
    "DECISION_MADE":           "A decision was made (subject=Issue, value=description)",
    "BUG_REPORTED":            "Bug reported in Component",
    "FEATURE_REQUESTED":       "Feature requested for Component",
    "REGRESSION_INTRODUCED":   "Regression introduced by commit/PR",
    "WORKAROUND_AVAILABLE":    "Workaround available for Issue",
    "BREAKING_CHANGE":         "Breaking change in Component",

    # People
    "PERSON_OWNS_COMPONENT":   "Person owns/maintains Component",
    "PERSON_REVIEWS_PR":       "Person reviewed PullRequest",
}


# ── Extraction Request/Response ───────────────────────────────────────────────

EXTRACTION_PROMPT = """You are an expert knowledge extractor for a software engineering memory system.

Given a GitHub issue (with comments), extract structured information.

Return ONLY valid JSON matching this schema exactly:

{{
  "entities": {{
    "persons": [
      {{"login": "string", "display_name": "string or null"}}
    ],
    "components": [
      {{"name": "string", "description": "string or null"}}
    ],
    "labels": [
      {{"name": "string"}}
    ]
  }},
  "claims": [
    {{
      "claim_type": "one of: {claim_types}",
      "subject_ref": "login or component_name or issue_number as string",
      "object_ref": "login or component_name or issue_number as string or null",
      "value": "string description or null",
      "confidence": 0.0-1.0,
      "evidence_excerpt": "exact quote from text ≤200 chars",
      "evidence_offset_start": integer,
      "evidence_offset_end": integer
    }}
  ],
  "summary": "1-2 sentence summary of the issue"
}}

Rules:
- Only extract claims clearly supported by the text
- confidence > 0.8 for explicit statements, 0.5-0.8 for inferred
- evidence_excerpt must be verbatim from the input text
- For DECISION_MADE: value should describe the decision clearly
- Components are vscode subsystems: editor, terminal, debugger, extensions, git, search, themes, etc.

Issue to extract from:
---
{issue_text}
---"""
