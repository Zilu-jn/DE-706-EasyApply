import hashlib
import json
import os
from datetime import UTC, datetime, timedelta

import pytest

from ingestion import github_list, greenhouse, lever
from ingestion.base import CollectionResult, RunContext, save_snapshot
from ingestion.run import write_outputs
from sql.db import connect, initialize
from tests.conftest import load_fixture
from transform.run import main as transform_main


@pytest.fixture
def postgres_dsn():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        if os.getenv("REQUIRE_POSTGRES_TESTS") == "1":
            pytest.fail("TEST_DATABASE_URL is required for PostgreSQL integration tests")
        pytest.skip("Set TEST_DATABASE_URL to a dedicated database ending in _test")
    with connect(dsn) as conn:
        if not conn.info.dbname.endswith("_test"):
            pytest.fail("Integration tests require a database name ending in _test")
        conn.execute("DROP SCHEMA IF EXISTS easyapply CASCADE")
        initialize(conn)
    yield dsn


@pytest.fixture
def db(postgres_dsn):
    with connect(postgres_dsn) as conn:
        conn.autocommit = True
        yield conn


@pytest.fixture
def batch_factory(tmp_path):
    def build(run_id="run-1", day=0, mutate=None, incomplete=(), only=None):
        ctx = RunContext(
            run_id, datetime(2026, 11, 19, 12, tzinfo=UTC) + timedelta(days=day), tmp_path
        )
        specs = [
            ("greenhouse", "lilasciences", "greenhouse_lilasciences.json"),
            ("lever", "tri", "lever_tri.json"),
            ("github", None, "github_listings.json"),
        ]
        collections = []
        for source, token, filename in specs:
            if only and source not in only:
                continue
            payload = load_fixture(filename)
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
                    commit_sha="fixture",
                    ctx=ctx,
                    snapshot_path=path,
                )
            if mutate:
                records = mutate(source, records)
            collections.append(
                CollectionResult(
                    source=source,
                    company_token=token,
                    records=records,
                    snapshot_path=path,
                    checksum=checksum,
                    http_status=503 if source in incomplete else 200,
                    expected_count=None if source == "lever" else len(records),
                    is_complete=source not in incomplete,
                    error="Fixture collection failed" if source in incomplete else None,
                )
            )
        write_outputs(collections, ctx)
        transform_main(["--run-id", run_id, "--data-dir", str(tmp_path)])
        return tmp_path, run_id

    return build


def count(db, table):
    from psycopg import sql

    return db.execute(
        sql.SQL("SELECT count(*) AS n FROM easyapply.{}").format(sql.Identifier(table))
    ).fetchone()["n"]


def edit_manifest(data_dir, run_id, edit):
    path = data_dir / "staging" / run_id / "manifest.json"
    manifest = json.loads(path.read_text())
    edit(manifest)
    path.write_text(json.dumps(manifest))


def describe_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()
