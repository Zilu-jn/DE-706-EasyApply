from __future__ import annotations

import argparse
import json
import os

from sql.db import connect, initialize
from sql.decisions import resolve_candidate, save_decision
from sql.load import load_run, register_feeds


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Initialize, load, and query EasyApply PostgreSQL")
    subs = parser.add_subparsers(dest="command", required=True)
    subs.add_parser("init")
    load = subs.add_parser("load")
    load.add_argument("--run-id", required=True)
    load.add_argument("--data-dir", default=os.getenv("DATA_DIR", "data"))
    subs.add_parser("summary")
    decision = subs.add_parser("decision")
    decision.add_argument("--student", required=True)
    decision.add_argument("--opportunity-key", required=True)
    decision.add_argument(
        "--status", required=True, choices=["saved", "applied", "dismissed", "reviewed"]
    )
    decision.add_argument("--notes")
    review = subs.add_parser("review")
    review.add_argument("--left", required=True, type=int)
    review.add_argument("--right", required=True, type=int)
    review.add_argument(
        "--status", required=True, choices=["confirmed_duplicate", "distinct", "dismissed"]
    )
    review.add_argument("--reviewer", required=True)
    review.add_argument("--notes")
    args = parser.parse_args(argv)
    if args.command == "init":
        from ingestion.base import load_config

        with connect() as conn:
            initialize(conn)
            register_feeds(conn, load_config())
        result = {"status": "initialized"}
    elif args.command == "load":
        result = load_run(args.data_dir, args.run_id)
    elif args.command == "decision":
        save_decision(args.student, args.opportunity_key, args.status, notes=args.notes)
        result = {"status": "decision_saved"}
    elif args.command == "review":
        resolve_candidate(
            args.left, args.right, args.status, reviewer=args.reviewer, notes=args.notes
        )
        result = {"status": "candidate_reviewed"}
    else:
        with connect() as conn:
            result = conn.execute("""
                SELECT (SELECT count(*) FROM easyapply.v_current_opportunities) AS opportunities,
                       (SELECT count(*) FROM easyapply.v_current_postings
                        WHERE is_present) AS postings,
                       (SELECT count(*) FROM easyapply.v_confirmed_duplicates)
                        AS cross_source_matches,
                       (SELECT count(*) FROM easyapply.v_pending_reviews) AS pending_reviews,
                       (SELECT count(*) FROM easyapply.v_posting_changes) AS changes,
                       (SELECT count(*) FROM easyapply.v_current_requirement_evidence) AS evidence
            """).fetchone()
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    main()
