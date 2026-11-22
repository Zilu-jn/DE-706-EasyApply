"""Run every adapter once and write the Stage 1 outputs.

    python -m ingestion.run                 # all sources
    python -m ingestion.run --only greenhouse

Outputs (see docs/data_contract.md):
    data/staging/{run_id}/raw_records.parquet
    data/staging/{run_id}/manifest.json

Each `collect_*` function is independent so Airflow can run them as parallel tasks.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import polars as pl
import requests

from ingestion import github_list, greenhouse, lever
from ingestion.base import CollectionResult, RawRecord, RunContext, load_config

log = logging.getLogger("easyapply.ingestion")

RAW_SCHEMA = {
    "source": pl.Utf8,
    "external_id": pl.Utf8,
    "company_token": pl.Utf8,
    "company": pl.Utf8,
    "title": pl.Utf8,
    "location": pl.Utf8,
    "apply_url": pl.Utf8,
    "description_html": pl.Utf8,
    "posted_at": pl.Datetime("us", "UTC"),
    "updated_at": pl.Datetime("us", "UTC"),
    "fetched_at": pl.Datetime("us", "UTC"),
    "run_id": pl.Utf8,
    "snapshot_path": pl.Utf8,
    "extra": pl.Utf8,
}


def collect_greenhouse(cfg: dict, ctx: RunContext, session=None) -> list[CollectionResult]:
    gh = cfg["greenhouse"]
    return [
        greenhouse.collect(b, ctx, cfg["http"], gh["base_url"], session=session)
        for b in gh["boards"]
    ]


def collect_lever(cfg: dict, ctx: RunContext, session=None) -> list[CollectionResult]:
    lv = cfg["lever"]
    return [
        lever.collect(c, ctx, cfg["http"], lv["base_url"], lv.get("page_size", 100), session)
        for c in lv["companies"]
    ]


def collect_github(cfg: dict, ctx: RunContext, session=None) -> list[CollectionResult]:
    return [github_list.collect(cfg["github_list"], ctx, cfg["http"], session=session)]


COLLECTORS = {
    "greenhouse": collect_greenhouse,
    "lever": collect_lever,
    "github": collect_github,
}


def records_to_frame(records: list[RawRecord]) -> pl.DataFrame:
    return pl.DataFrame([r.to_dict() for r in records], schema=RAW_SCHEMA)


def write_outputs(results: list[CollectionResult], ctx: RunContext) -> Path:
    out_dir = ctx.data_dir / "staging" / ctx.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    records = [r for res in results for r in res.records]
    records_to_frame(records).write_parquet(out_dir / "raw_records.parquet")
    manifest = {
        "run_id": ctx.run_id,
        "fetched_at": ctx.fetched_at.isoformat(),
        "total_records": len(records),
        "all_complete": all(r.is_complete for r in results),
        "collections": [r.manifest_entry() for r in results],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return out_dir


def main(argv: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=list(COLLECTORS), action="append")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "data"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cfg = load_config()
    ctx = RunContext.new(args.data_dir)
    session = requests.Session()
    results: list[CollectionResult] = []
    for name in args.only or COLLECTORS:
        results.extend(COLLECTORS[name](cfg, ctx, session))

    for r in results:
        status = "ok" if r.is_complete else f"INCOMPLETE ({r.error})"
        log.info(
            "%-10s %-14s %5d records  %s", r.source, r.company_token or "-", len(r.records), status
        )
    out_dir = write_outputs(results, ctx)
    log.info("Wrote %s", out_dir)
    return out_dir


if __name__ == "__main__":
    main()
