import json

import polars as pl
import psycopg
import pytest

from sql.db import initialize
from sql.decisions import resolve_candidate, save_decision
from sql.load import load_run
from tests.conftest import load_fixture
from tests.integration.conftest import count, edit_manifest

pytestmark = pytest.mark.postgres


def load(batch, dsn):
    return load_run(*batch, dsn=dsn)


def change_description(source, records):
    if source == "greenhouse" and records:
        records[0].description_html = "<p>Python and SQL. Visa sponsorship is available.</p>"
    return records


def test_fixture_pipeline_loads_real_source_samples(db, postgres_dsn, batch_factory):
    result = load(batch_factory(), postgres_dsn)
    assert result["status"] == "loaded"
    assert count(db, "v_confirmed_duplicates") == 2
    assert count(db, "v_pending_reviews") >= 1
    assert count(db, "v_current_opportunities") < count(db, "postings")
    assert count(db, "v_current_requirement_evidence") > 0
    assert count(db, "source_feeds") == 11
    assert (
        db.execute(
            "SELECT count(*) AS n FROM easyapply.v_source_health "
            "WHERE health_status = 'never_collected'"
        ).fetchone()["n"]
        == 8
    )


def test_migrations_and_batch_replay_are_idempotent(db, postgres_dsn, batch_factory):
    initialize(db)
    batch = batch_factory()
    load(batch, postgres_dsn)
    tables = [
        "opportunities",
        "postings",
        "posting_versions",
        "posting_observations",
        "review_candidates",
        "requirement_evidence",
    ]
    before = {t: count(db, t) for t in tables}
    assert load(batch, postgres_dsn)["status"] == "already_loaded"
    assert {t: count(db, t) for t in tables} == before


def test_unchanged_run_adds_observations_without_versions(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    versions, observations = count(db, "posting_versions"), count(db, "posting_observations")
    load(batch_factory("run-2", 1), postgres_dsn)
    assert count(db, "posting_versions") == versions
    assert count(db, "posting_observations") == 2 * observations
    assert count(db, "v_posting_changes") == 0
    assert db.execute(
        "SELECT DISTINCT first_seen_run FROM easyapply.v_current_opportunities"
    ).fetchall() == [{"first_seen_run": "run-1"}]


def test_content_changes_and_reversions_preserve_transitions(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    before = count(db, "posting_versions")
    load(batch_factory("run-2", 1, change_description), postgres_dsn)
    load(batch_factory("run-3", 2), postgres_dsn)
    assert count(db, "posting_versions") == before + 1
    assert count(db, "v_posting_changes") == 2
    assert db.execute(
        "SELECT DISTINCT run_id FROM easyapply.v_posting_changes ORDER BY run_id"
    ).fetchall() == [{"run_id": "run-2"}, {"run_id": "run-3"}]


def test_extra_metadata_changes_without_description_hash_changes(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)

    def mutate(source, records):
        if source == "github":
            extra = json.loads(records[0].extra)
            extra["sponsorship"] = "Updated source category"
            records[0].extra = json.dumps(extra)
        return records

    load(batch_factory("run-2", 1, mutate), postgres_dsn)
    changes = db.execute("SELECT * FROM easyapply.v_posting_changes").fetchall()
    assert len(changes) == 1
    assert changes[0]["content_hash"] == changes[0]["previous_content_hash"]
    assert changes[0]["extra"] != changes[0]["previous_extra"]
    assert (
        db.execute(
            "SELECT count(*) AS n FROM easyapply.v_current_requirement_evidence "
            "WHERE excerpt = 'Updated source category'"
        ).fetchone()["n"]
        == 1
    )


def test_incomplete_feed_preserves_last_published_state(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    before = db.execute(
        "SELECT external_id, state_hash FROM easyapply.v_current_postings "
        "WHERE source = 'greenhouse' ORDER BY external_id"
    ).fetchall()
    load(batch_factory("run-2", 1, change_description, incomplete=("greenhouse",)), postgres_dsn)
    after = db.execute(
        "SELECT external_id, state_hash FROM easyapply.v_current_postings "
        "WHERE source = 'greenhouse' ORDER BY external_id"
    ).fetchall()
    assert before == after
    health = db.execute(
        "SELECT * FROM easyapply.v_source_health WHERE feed_key = 'greenhouse:lilasciences'"
    ).fetchone()
    assert health["health_status"] == "incomplete"
    assert health["last_success_run"] == "run-1"
    assert count(db, "v_posting_changes") == 0


def test_complete_empty_employer_feed_marks_absence_at_feed_scope(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    load(batch_factory("run-2", 1, lambda s, r: [] if s == "greenhouse" else r), postgres_dsn)
    assert (
        db.execute(
            "SELECT count(*) AS n FROM easyapply.v_current_postings "
            "WHERE source = 'greenhouse' AND is_present"
        ).fetchone()["n"]
        == 0
    )
    assert count(db, "v_current_opportunities") > 0
    assert count(db, "postings") > count(db, "v_current_opportunities")


def test_partial_source_run_does_not_remove_other_feeds(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    before = count(db, "v_current_opportunities")
    load(batch_factory("run-2", 1, only=("lever",)), postgres_dsn)
    assert count(db, "v_current_opportunities") == before


def test_out_of_order_batches_do_not_regress_current_state(db, postgres_dsn, batch_factory):
    load(batch_factory("run-new", 2, change_description), postgres_dsn)
    load(batch_factory("run-old", 0), postgres_dsn)
    assert db.execute("SELECT DISTINCT run_id FROM easyapply.v_current_postings").fetchall() == [
        {"run_id": "run-new"}
    ]
    assert db.execute(
        "SELECT DISTINCT first_seen_run FROM easyapply.v_current_opportunities"
    ).fetchall() == [{"first_seen_run": "run-old"}]
    assert count(db, "v_posting_changes") == 1


def test_student_decisions_survive_loads_and_flag_changes(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    changed_id = str(load_fixture("greenhouse_lilasciences.json")["jobs"][0]["id"])
    key = db.execute(
        "SELECT opportunity_key FROM easyapply.v_current_postings "
        "WHERE source = 'greenhouse' AND external_id = %s",
        (changed_id,),
    ).fetchone()["opportunity_key"]
    save_decision("student-demo", key, "saved", notes="SQL practice", dsn=postgres_dsn)
    load(batch_factory("run-2", 1, change_description), postgres_dsn)
    assert count(db, "student_decisions") == 1
    assert count(db, "v_student_follow_up") == 1
    save_decision("student-demo", key, "reviewed", dsn=postgres_dsn)
    assert count(db, "v_student_follow_up") == 0


def test_review_outcomes_survive_new_runs(db, postgres_dsn, batch_factory):
    load(batch_factory(), postgres_dsn)
    pair = db.execute("SELECT * FROM easyapply.v_pending_reviews LIMIT 1").fetchone()
    resolve_candidate(
        pair["right_posting_id"],
        pair["left_posting_id"],
        "distinct",
        reviewer="team",
        dsn=postgres_dsn,
    )
    load(batch_factory("run-2", 1), postgres_dsn)
    row = db.execute(
        "SELECT status, reviewer FROM easyapply.review_candidates "
        "WHERE left_posting_id = %s AND right_posting_id = %s",
        (pair["left_posting_id"], pair["right_posting_id"]),
    ).fetchone()
    assert row == {"status": "distinct", "reviewer": "team"}


def test_database_failure_rolls_back_records_and_keeps_audit(db, postgres_dsn, batch_factory):
    path, run_id = batch_factory()
    file = path / "processed" / run_id / "postings.parquet"
    frame = (
        pl.read_parquet(file)
        .with_row_index("i")
        .with_columns(
            pl.when(pl.col("i") == 1)
            .then(pl.lit("bad-hash"))
            .otherwise(pl.col("content_hash"))
            .alias("content_hash")
        )
        .drop("i")
    )
    frame.write_parquet(file)
    with pytest.raises(psycopg.errors.CheckViolation):
        load((path, run_id), postgres_dsn)
    assert count(db, "postings") == 0
    assert count(db, "opportunities") == 0
    assert count(db, "source_collections") == 3
    assert db.execute("SELECT status, error_type FROM easyapply.pipeline_runs").fetchone() == {
        "status": "failed",
        "error_type": "CheckViolation",
    }


def test_reused_run_id_with_changed_bytes_is_rejected(db, postgres_dsn, batch_factory):
    batch = batch_factory()
    load(batch, postgres_dsn)
    edit_manifest(*batch, lambda m: m.update(extra_note="changed"))
    with pytest.raises(ValueError, match="different input bytes"):
        load(batch, postgres_dsn)
    assert count(db, "pipeline_runs") == 1


def test_schema_constraints_reject_orphans(db):
    with pytest.raises(psycopg.errors.ForeignKeyViolation), db.transaction():
        db.execute(
            "INSERT INTO easyapply.student_decisions "
            "(student_key, opportunity_key, decision, reviewed_signature) "
            "VALUES ('student', 'missing', 'saved', 'signature')"
        )


def test_recording_demo_runs_end_to_end(db, postgres_dsn, tmp_path):
    from sql.demo import run_demo

    result = run_demo(tmp_path, dsn=postgres_dsn)
    assert result["cross_source_matches"] == 2
    assert result["changes"] == 1
    assert result["student_follow_up"] == 1
    assert result["greenhouse_health"] == "incomplete"
    assert result["greenhouse_last_success"] == result["updated_run"]
    assert result["replay"] == "already_loaded"


def test_example_queries_execute(db, postgres_dsn, batch_factory):
    from sql.db import SQL_DIR

    load(batch_factory(), postgres_dsn)
    for query in (SQL_DIR / "example_queries.sql").read_text().split(";"):
        if query.strip():
            db.execute(query).fetchall()


def test_repository_commit_changes_are_lineage_not_posting_versions(
    db, postgres_dsn, batch_factory
):
    load(batch_factory(), postgres_dsn)
    before = count(db, "posting_versions")

    def mutate(source, records):
        if source == "github":
            for record in records:
                extra = json.loads(record.extra)
                extra["commit_sha"] = "new-repository-commit"
                record.extra = json.dumps(extra)
        return records

    load(batch_factory("run-2", 1, mutate), postgres_dsn)
    assert count(db, "posting_versions") == before
    assert count(db, "v_posting_changes") == 0
    commits = db.execute(
        "SELECT DISTINCT source_extra->>'commit_sha' AS sha "
        "FROM easyapply.v_current_postings WHERE source = 'github'"
    ).fetchall()
    assert commits == [{"sha": "new-repository-commit"}]


def test_source_updated_timestamp_is_preserved_per_observation(db, postgres_dsn, batch_factory):
    from datetime import UTC, datetime

    load(batch_factory(), postgres_dsn)
    before = count(db, "posting_versions")
    updated = datetime(2026, 11, 20, tzinfo=UTC)

    def mutate(source, records):
        if source == "greenhouse":
            for record in records:
                record.updated_at = updated
        return records

    load(batch_factory("run-2", 1, mutate), postgres_dsn)
    assert count(db, "posting_versions") == before
    times = db.execute(
        "SELECT DISTINCT source_updated_at FROM easyapply.v_current_postings "
        "WHERE source = 'greenhouse'"
    ).fetchall()
    assert times == [{"source_updated_at": updated}]


def test_failed_batch_can_retry_identical_inputs(db, postgres_dsn, batch_factory, monkeypatch):
    import sql.load as loader

    original = loader._load_posting
    batch = batch_factory()

    def fail_once(*args):
        raise RuntimeError("Simulated transient database-load failure")

    monkeypatch.setattr(loader, "_load_posting", fail_once)
    with pytest.raises(RuntimeError):
        load(batch, postgres_dsn)
    assert count(db, "postings") == 0
    monkeypatch.setattr(loader, "_load_posting", original)
    assert load(batch, postgres_dsn)["status"] == "loaded"
    assert count(db, "pipeline_runs") == 1
    assert (
        db.execute("SELECT error_type FROM easyapply.pipeline_runs").fetchone()["error_type"]
        is None
    )
