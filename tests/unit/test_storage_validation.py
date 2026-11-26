import pytest

from ingestion.base import load_config
from sql.evidence import extract_evidence
from sql.load import _read_batch


@pytest.mark.parametrize(
    "statement,expected",
    [
        ("Visa sponsorship is available.", "explicit_available"),
        ("We cannot provide visa sponsorship.", "explicit_unavailable"),
        ("No sponsorship is offered.", "explicit_unavailable"),
        ("Sponsorship may depend on the role.", "statement"),
        ("We do not require sponsorship experience.", "statement"),
    ],
)
def test_sponsorship_rules_keep_literal_evidence(statement, expected):
    rows = extract_evidence(statement, {})
    assert rows == [
        {
            "attribute": "sponsorship",
            "value": expected,
            "excerpt": statement,
            "evidence_field": "description_text",
            "method": "rules-v1",
        }
    ]


def test_skill_mentions_and_source_metadata_are_labeled_separately():
    text = "Python is helpful. SQL is required. The letter r is lowercase."
    rows = extract_evidence(text, {"sponsorship": "Unknown"})
    assert {r["value"] for r in rows if r["attribute"] == "skill"} == {"python", "sql"}
    assert all(r["excerpt"] in text for r in rows if r["evidence_field"] == "description_text")
    assert rows[-1]["evidence_field"] == "extra.sponsorship"
    assert extract_evidence(None, {}) == []


@pytest.mark.parametrize("run_id", ["../other", "/tmp/other", "..", ".", ""])
def test_invalid_run_directory_is_rejected(tmp_path, run_id):
    with pytest.raises(ValueError, match="single directory"):
        _read_batch(tmp_path, run_id, load_config())
