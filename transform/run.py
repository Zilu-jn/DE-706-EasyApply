"""Clean and deduplicate one ingestion run.

    python -m transform.run                    # latest run in data/staging
    python -m transform.run --run-id <run_id>

Outputs (see docs/data_contract.md):
    data/processed/{run_id}/postings.parquet
    data/processed/{run_id}/review_candidates.parquet
    data/processed/{run_id}/summary.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import polars as pl

from ingestion.base import load_config
from transform.dedup import assign_opportunities, dedup_within_source, find_review_candidates
from transform.normalize import normalize_frame

log = logging.getLogger("easyapply.transform")


def latest_run_id(data_dir: Path) -> str:
    runs = sorted(p.name for p in (data_dir / "staging").iterdir() if p.is_dir())
    if not runs:
        raise FileNotFoundError(f"No ingestion runs under {data_dir / 'staging'}")
    return runs[-1]


def transform(
    raw: pl.DataFrame, target_role_keywords: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Pure pipeline step: raw records in, (postings, review candidates) out."""
    cleaned = normalize_frame(raw, target_role_keywords)
    deduped = dedup_within_source(cleaned)
    postings = assign_opportunities(deduped).sort(["source", "external_id"])
    candidates = find_review_candidates(postings)
    return postings, candidates


def summarize(raw: pl.DataFrame, postings: pl.DataFrame, candidates: pl.DataFrame) -> dict:
    cross = postings.filter(pl.col("n_sources") > 1)
    return {
        "input_records": raw.height,
        "postings_after_within_source_dedup": postings.height,
        "unique_opportunities": postings["opportunity_id"].n_unique(),
        "target_role_postings": int(postings["is_target_role"].sum()),
        "cross_source_opportunities": cross["opportunity_id"].n_unique(),
        "cross_source_by_method": dict(
            cross.group_by("match_method").agg(pl.col("opportunity_id").n_unique()).iter_rows()
        ),
        "review_candidates": candidates.height,
        "by_source": dict(postings.group_by("source").len().iter_rows()),
    }


def main(argv: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "data"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    data_dir = Path(args.data_dir)
    run_id = args.run_id or latest_run_id(data_dir)
    raw = pl.read_parquet(data_dir / "staging" / run_id / "raw_records.parquet")
    cfg = load_config()

    postings, candidates = transform(raw, cfg["target_role_keywords"])

    out_dir = data_dir / "processed" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    postings.write_parquet(out_dir / "postings.parquet")
    candidates.write_parquet(out_dir / "review_candidates.parquet")
    summary = summarize(raw, postings, candidates)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    log.info("Summary: %s", json.dumps(summary))
    log.info("Wrote %s", out_dir)
    return out_dir


if __name__ == "__main__":
    main()
