from __future__ import annotations

import re

METHOD = "rules-v1"
SKILLS = {
    "python": r"\bpython\b",
    "sql": r"\bsql\b",
    "r": r"\bR\b",
    "java": r"\bjava\b",
    "spark": r"\b(?:pyspark|apache spark|spark)\b",
    "tableau": r"\btableau\b",
    "power_bi": r"\bpower\s*bi\b",
    "excel": r"\bexcel\b",
    "aws": r"\b(?:aws|amazon web services)\b",
    "docker": r"\bdocker\b",
    "airflow": r"\bairflow\b",
}
NEGATIVE = re.compile(
    r"\b(?:no|without)\s+(?:visa\s+)?sponsorship\b|"
    r"\b(?:cannot|can't|will not|do not|does not|unable to)\s+"
    r"(?:provide|offer|support|sponsor)\b.{0,40}\bsponsorship\b|"
    r"\bsponsorship\s+(?:is\s+)?(?:not available|not provided|not offered)\b",
    re.I,
)
POSITIVE = re.compile(
    r"\b(?:visa\s+)?sponsorship\s+(?:is\s+)?(?:available|provided|offered)\b|"
    r"\bwe\s+(?:will|can|do)\s+(?:provide|offer|support)\s+(?:visa\s+)?sponsorship\b",
    re.I,
)


def extract_evidence(description: str | None, extra: dict) -> list[dict]:
    """Retain literal excerpts; skill mentions do not imply mandatory requirements."""
    rows = []
    text = description or ""
    for skill, pattern in SKILLS.items():
        flags = 0 if skill == "r" else re.I
        match = re.search(pattern, text, flags)
        if match:
            excerpt = text[max(0, match.start() - 100) : min(len(text), match.end() + 150)]
            rows.append(_row("skill", skill, excerpt, "description_text"))
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", text):
        if not re.search(r"sponsor(?:ship)?", sentence, re.I):
            continue
        value = "statement"
        if NEGATIVE.search(sentence):
            value = "explicit_unavailable"
        elif POSITIVE.search(sentence):
            value = "explicit_available"
        rows.append(_row("sponsorship", value, sentence, "description_text"))
    if extra.get("sponsorship") not in (None, ""):
        rows.append(
            _row("sponsorship", "source_metadata", str(extra["sponsorship"]), "extra.sponsorship")
        )
    return rows


def _row(attribute: str, value: str, excerpt: str, field: str) -> dict:
    return {
        "attribute": attribute,
        "value": value,
        "excerpt": excerpt,
        "evidence_field": field,
        "method": METHOD,
    }
