from __future__ import annotations

import argparse
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from ingestion import github_list, greenhouse, lever
from ingestion.base import CollectionResult, RunContext, save_snapshot
from ingestion.run import write_outputs
from sql.db import SQL_DIR, connect, initialize
from sql.decisions import save_decision
from sql.load import load_run
from transform.run import main as transform_main


def build_batch(data_dir: Path, run_id: str, at: datetime, *, edited=False, failed=False):
    ctx = RunContext(run_id, at, data_dir)
    results = []
    for source, token, filename in [
        ("greenhouse", "lilasciences", "greenhouse_lilasciences.json"),
        ("lever", "tri", "lever_tri.json"),
        ("github", None, "github_listings.json"),
    ]:
        payload = json.loads((SQL_DIR.parent / "tests" / "fixtures" / filename).read_text())
        if source == "greenhouse" and edited:
            payload["jobs"][0]["content"] += (
                "&lt;p&gt;Synthetic demo update: SQL and Docker experience. "
                "Visa sponsorship is not available.&lt;/p&gt;"
            )
        path, checksum = save_snapshot(payload, source, token or "listings", ctx)
        if source == "greenhouse":
            records = greenhouse.parse_jobs(
                payload, token=token, company="Lila Sciences", ctx=ctx, snapshot_path=path
            )
        elif source == "lever":
            records = lever.parse_postings(
                payload,
                token=token,
                company="Toyota Research Institute",
                ctx=ctx,
                snapshot_path=path,
            )
        else:
            records = github_list.parse_listings(
                payload,
                categories=["AI/ML/Data", "Data Science, AI & Machine Learning"],
                active_only=True,
                repo="SimplifyJobs/Summer2027-Internships",
                commit_sha="saved-fixture",
                ctx=ctx,
                snapshot_path=path,
            )
        incomplete = failed and source == "greenhouse"
        results.append(
            CollectionResult(
                source=source,
                company_token=token,
                records=[] if incomplete else records,
                snapshot_path=path,
                checksum=checksum,
                http_status=503 if incomplete else 200,
                expected_count=None if source == "lever" else len(records),
                is_complete=not incomplete,
                error="Synthetic demo feed failure" if incomplete else None,
            )
        )
    write_outputs(results, ctx)
    transform_main(["--data-dir", str(data_dir), "--run-id", run_id])


def run_demo(data_dir: Path, *, dsn: str | None = None) -> dict:
    with connect(dsn) as conn:
        if not conn.info.dbname.endswith(("_demo", "_test")):
            raise ValueError("Use a dedicated database ending in _demo for synthetic demo updates")
        initialize(conn)
    prefix = "demo-" + uuid.uuid4().hex[:10]
    now = datetime.now(UTC) - timedelta(seconds=3)
    baseline, updated, failed = [f"{prefix}-{i}" for i in (1, 2, 3)]
    build_batch(data_dir, baseline, now)
    first = load_run(data_dir, baseline, dsn=dsn)
    frame = pl.read_parquet(data_dir / "processed" / baseline / "postings.parquet")
    original_id = str(
        json.loads(
            (SQL_DIR.parent / "tests" / "fixtures" / "greenhouse_lilasciences.json").read_text()
        )["jobs"][0]["id"]
    )
    key = frame.filter((pl.col("source") == "greenhouse") & (pl.col("external_id") == original_id))[
        "opportunity_key"
    ][0]
    save_decision(prefix, key, "saved", notes="Demo student decision", dsn=dsn)
    build_batch(data_dir, updated, now + timedelta(seconds=1), edited=True)
    load_run(data_dir, updated, dsn=dsn)
    build_batch(data_dir, failed, now + timedelta(seconds=2), failed=True)
    load_run(data_dir, failed, dsn=dsn)
    replay = load_run(data_dir, updated, dsn=dsn)
    with connect(dsn) as conn:
        result = conn.execute(
            """
            SELECT (SELECT count(*) FROM easyapply.v_current_opportunities) AS opportunities,
                   (SELECT count(*) FROM easyapply.v_confirmed_duplicates) AS cross_source_matches,
                   (SELECT count(*) FROM easyapply.v_pending_reviews) AS pending_reviews,
                   (SELECT count(*) FROM easyapply.v_posting_changes WHERE run_id = %s) AS changes,
                   (SELECT count(*) FROM easyapply.v_student_follow_up WHERE student_key = %s)
                       AS student_follow_up,
                   (SELECT health_status FROM easyapply.v_source_health
                    WHERE feed_key = 'greenhouse:lilasciences') AS greenhouse_health,
                   (SELECT last_success_run FROM easyapply.v_source_health
                    WHERE feed_key = 'greenhouse:lilasciences') AS greenhouse_last_success
        """,
            (updated, prefix),
        ).fetchone()
    result.update(
        {
            "baseline_load": first["status"],
            "replay": replay["status"],
            "updated_run": updated,
            "failed_run": failed,
            "data_note": "Saved real source samples; update and failure are synthetic demo cases.",
        }
    )
    return result


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Demonstrate storage using saved source fixtures")
    parser.add_argument("--data-dir", type=Path, default=Path("data/demo"))
    args = parser.parse_args(argv)
    result = run_demo(args.data_dir)
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    main()
