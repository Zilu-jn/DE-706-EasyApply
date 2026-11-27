"""Evaluate duplicate detection against a manually labeled sample of posting pairs.

The sample (labeled_pairs.csv) was labeled by hand from the saved source
responses in tests/fixtures: each row says whether two records are the same
opportunity ("duplicate") or not ("distinct"). The pipeline's prediction is
"same opportunity_id after transform". The report separates:

- missed duplicates: labeled duplicate, but the pipeline kept them apart
  (and whether the review queue at least caught the pair), and
- incorrect matches: labeled distinct, but the pipeline merged them.

Run it:

    uv run python -m evaluation.dedup_accuracy [--json]

Exits non-zero if there is any incorrect match or a missed duplicate that the
review queue did not catch, so CI fails when accuracy regresses.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion import github_list, greenhouse, lever
from ingestion.base import RunContext, load_config
from ingestion.run import records_to_frame
from transform.run import transform

FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
LABELED_PAIRS = Path(__file__).resolve().parent / "labeled_pairs.csv"


def _load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


def build_fixture_frame() -> pl.DataFrame:
    """The same saved source responses the end-to-end tests use. No network."""
    ctx = RunContext(run_id="eval", fetched_at=datetime.now(UTC), data_dir=Path("."))
    records = []
    records += greenhouse.parse_jobs(
        _load_fixture("greenhouse_lilasciences.json"),
        token="lilasciences",
        company="Lila Sciences",
        ctx=ctx,
        snapshot_path="gh.json",
    )
    records += lever.parse_postings(
        _load_fixture("lever_tri.json"),
        token="tri",
        company="Toyota Research Institute",
        ctx=ctx,
        snapshot_path="lv.json",
    )
    records += github_list.parse_listings(
        _load_fixture("github_listings.json"),
        categories=["AI/ML/Data", "Data Science, AI & Machine Learning"],
        active_only=True,
        repo="SimplifyJobs/Summer2027-Internships",
        commit_sha="abc",
        ctx=ctx,
        snapshot_path="gl.json",
    )
    return records_to_frame(records)


def load_labeled_pairs(path: Path = LABELED_PAIRS) -> list[dict]:
    with open(path, newline="") as f:
        pairs = list(csv.DictReader(f))
    for row in pairs:
        if row["label"] not in ("duplicate", "distinct"):
            raise ValueError(f"Unknown label {row['label']!r} in {path}")
    return pairs


def evaluate(
    postings: pl.DataFrame, candidates: pl.DataFrame, pairs: list[dict]
) -> dict:
    """Compare pipeline matches against human labels for each pair."""
    opportunity_of = {
        (source, external_id): opp_id
        for source, external_id, opp_id in postings.select(
            "source", "external_id", "opportunity_id"
        ).iter_rows()
    }
    in_review = {
        frozenset([(ls, li), (rs, ri)])
        for ls, li, rs, ri in candidates.select(
            "left_source", "left_external_id", "right_source", "right_external_id"
        ).iter_rows()
    }

    tp = tn = 0
    missed_duplicates: list[dict] = []
    incorrect_matches: list[dict] = []
    for row in pairs:
        left = (row["left_source"], row["left_external_id"])
        right = (row["right_source"], row["right_external_id"])
        for key in (left, right):
            if key not in opportunity_of:
                raise KeyError(f"Labeled pair references unknown record {key}")
        predicted_same = opportunity_of[left] == opportunity_of[right]
        labeled_same = row["label"] == "duplicate"
        if labeled_same and predicted_same:
            tp += 1
        elif not labeled_same and not predicted_same:
            tn += 1
        elif labeled_same and not predicted_same:
            missed_duplicates.append(
                {**row, "in_review_queue": frozenset([left, right]) in in_review}
            )
        else:
            incorrect_matches.append(dict(row))

    fn, fp = len(missed_duplicates), len(incorrect_matches)
    recovered = sum(1 for m in missed_duplicates if m["in_review_queue"])
    return {
        "pairs": len(pairs),
        "true_positives": tp,
        "true_negatives": tn,
        "missed_duplicates": missed_duplicates,
        "incorrect_matches": incorrect_matches,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        # counting a pair that lands in the human review queue as caught
        "review_adjusted_recall": (tp + recovered) / (tp + fn) if tp + fn else None,
    }


def evaluate_on_fixtures() -> dict:
    postings, candidates = transform(
        build_fixture_frame(), load_config()["target_role_keywords"]
    )
    return evaluate(postings, candidates, load_labeled_pairs())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)

    report = evaluate_on_fixtures()
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"Labeled pairs:          {report['pairs']}")
        print(f"Correctly matched:      {report['true_positives']}")
        print(f"Correctly kept apart:   {report['true_negatives']}")
        print(f"Precision:              {report['precision']:.3f}")
        print(f"Recall:                 {report['recall']:.3f}")
        print(f"Review-adjusted recall: {report['review_adjusted_recall']:.3f}")
        for m in report["missed_duplicates"]:
            caught = "caught by review queue" if m["in_review_queue"] else "NOT CAUGHT"
            print(
                f"missed duplicate ({caught}): "
                f"{m['left_source']}:{m['left_external_id']} vs "
                f"{m['right_source']}:{m['right_external_id']} -- {m['notes']}"
            )
        for m in report["incorrect_matches"]:
            print(
                f"INCORRECT MATCH: {m['left_source']}:{m['left_external_id']} vs "
                f"{m['right_source']}:{m['right_external_id']} -- {m['notes']}"
            )

    uncaught = [m for m in report["missed_duplicates"] if not m["in_review_queue"]]
    return 1 if report["incorrect_matches"] or uncaught else 0


if __name__ == "__main__":
    sys.exit(main())
