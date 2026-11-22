"""Build small, real test fixtures from a collected run.

    python -m tests.fixtures.build_fixtures [--run-id RUN_ID]

Picks a handful of real postings so tests cover the interesting cases:
- Greenhouse jobs that also appear in the GitHub list (confident cross-source match)
- Lever jobs with the same title but different posting IDs (review candidate)
- GitHub listings that don't match anything we collect

Tests never call live APIs; they only read these files.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

FIXTURE_DIR = Path(__file__).parent
DATA_DIR = Path("data")


def _load(path: str):
    return json.loads(Path(path).read_text())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    run_id = args.run_id or sorted(p.name for p in (DATA_DIR / "processed").iterdir())[-1]
    postings = pl.read_parquet(DATA_DIR / "processed" / run_id / "postings.parquet")
    candidates = pl.read_parquet(DATA_DIR / "processed" / run_id / "review_candidates.parquet")

    # Greenhouse: 2 jobs matched to GitHub + 1 unmatched, all from one board
    matched = postings.filter((pl.col("source") == "greenhouse") & (pl.col("n_sources") > 1))
    board = matched["company_token"][0]
    gh_rows = postings.filter(
        (pl.col("source") == "greenhouse") & (pl.col("company_token") == board)
    )
    gh_ids = matched.filter(pl.col("company_token") == board)["external_id"].head(2).to_list()
    gh_ids += gh_rows.filter(pl.col("n_sources") == 1)["external_id"].head(1).to_list()
    gh_snapshot = _load(gh_rows["snapshot_path"][0])
    gh_jobs = [j for j in gh_snapshot["jobs"] if str(j["id"]) in gh_ids]
    gh_fixture = {"jobs": gh_jobs, "meta": {"total": len(gh_jobs)}}

    # Lever: one review-candidate pair (same title, different IDs) + 1 other posting
    pair = candidates.filter(pl.col("left_source") != pl.col("right_source")).row(0, named=True)
    lever_side = "left" if pair["left_source"] == "lever" else "right"
    other_side = "right" if lever_side == "left" else "left"
    lv_ids = [pair[f"{lever_side}_external_id"]]
    lv_row = postings.filter((pl.col("source") == "lever") & (pl.col("external_id") == lv_ids[0]))
    lv_token = lv_row["company_token"][0]
    lv_snapshot = _load(lv_row["snapshot_path"][0])
    extra = next(p for p in lv_snapshot if p["id"] not in lv_ids)
    lv_fixture = [p for p in lv_snapshot if p["id"] in lv_ids] + [extra]

    # GitHub: the listings for the matched Greenhouse jobs, the candidate's other side,
    # and two unrelated listings
    gh_keys = {f"greenhouse:{i}" for i in gh_ids}
    gh_list_rows = postings.filter(pl.col("source") == "github")
    wanted = set(gh_list_rows.filter(pl.col("ats_job_key").is_in(gh_keys))["external_id"].to_list())
    wanted.add(pair[f"{other_side}_external_id"])
    wanted |= set(gh_list_rows.filter(pl.col("n_sources") == 1)["external_id"].head(2).to_list())
    gl_snapshot = _load(gh_list_rows["snapshot_path"][0])
    gl_fixture = [item for item in gl_snapshot if str(item["id"]) in wanted]

    out = {
        f"greenhouse_{board}.json": gh_fixture,
        f"lever_{lv_token}.json": lv_fixture,
        "github_listings.json": gl_fixture,
    }
    for name, payload in out.items():
        (FIXTURE_DIR / name).write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        print(f"wrote {name}")
    print(json.dumps({"greenhouse_board": board, "lever_token": lv_token}))


if __name__ == "__main__":
    main()
