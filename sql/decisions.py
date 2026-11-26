from __future__ import annotations

from sql.db import connect


def save_decision(
    student_key: str,
    opportunity_key: str,
    decision: str,
    *,
    notes: str | None = None,
    dsn: str | None = None,
) -> None:
    if not student_key.strip() or decision not in {"saved", "applied", "dismissed", "reviewed"}:
        raise ValueError("Provide a student key and a valid decision")
    with connect(dsn) as conn:
        opportunity = conn.execute(
            """
            SELECT current_signature FROM easyapply.v_current_opportunities
            WHERE opportunity_key = %s
        """,
            (opportunity_key,),
        ).fetchone()
        if not opportunity:
            raise ValueError("Opportunity is not currently published")
        conn.execute(
            """
            INSERT INTO easyapply.student_decisions
                (student_key, opportunity_key, decision, reviewed_signature, notes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (student_key, opportunity_key) DO UPDATE
            SET decision = EXCLUDED.decision, reviewed_signature = EXCLUDED.reviewed_signature,
                notes = EXCLUDED.notes, decided_at = CURRENT_TIMESTAMP
        """,
            (student_key, opportunity_key, decision, opportunity["current_signature"], notes),
        )


def resolve_candidate(
    left_posting_id: int,
    right_posting_id: int,
    status: str,
    *,
    reviewer: str,
    notes: str | None = None,
    dsn: str | None = None,
) -> None:
    if not reviewer.strip() or status not in {"confirmed_duplicate", "distinct", "dismissed"}:
        raise ValueError("Provide a reviewer and a valid review outcome")
    left, right = sorted([left_posting_id, right_posting_id])
    with connect(dsn) as conn:
        result = conn.execute(
            """
            UPDATE easyapply.review_candidates
            SET status = %s, reviewer = %s, notes = %s, reviewed_at = CURRENT_TIMESTAMP
            WHERE left_posting_id = %s AND right_posting_id = %s
        """,
            (status, reviewer, notes, left, right),
        )
        if result.rowcount != 1:
            raise ValueError("Review candidate does not exist")
