from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import psycopg
from psycopg.types.json import Jsonb

from ingestion.base import load_config
from sql.db import connect
from sql.evidence import extract_evidence

VERSION_FIELDS = (
    "content_hash",
    "company",
    "company_normalized",
    "title",
    "title_normalized",
    "location",
    "location_normalized",
    "apply_url",
    "apply_url_normalized",
    "ats_job_key",
    "description_text",
    "description_html",
    "is_target_role",
    "posted_at",
    "updated_at",
    "extra",
)
REQUIRED = {
    "source",
    "external_id",
    "company_token",
    "company",
    "title",
    "run_id",
    "fetched_at",
    "snapshot_path",
    "opportunity_key",
    "opportunity_id",
    "content_hash",
    "match_method",
    "is_target_role",
    "extra",
}
CANDIDATE_FIELDS = {
    "left_source",
    "left_external_id",
    "right_source",
    "right_external_id",
    "title_score",
    "location_overlap",
}


def utc(value) -> datetime:
    dt = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    if dt.tzinfo is None:
        raise ValueError("Collection timestamps must include a timezone")
    return dt.astimezone(UTC)


def feed_key(source: str, token: str | None, config: dict) -> str:
    if source == "github":
        token = config["github_list"]["repo"]
    if not token:
        raise ValueError("Employer collections require company_token")
    return f"{source}:{token}"


def register_feeds(conn: psycopg.Connection, config: dict) -> None:
    rows = [
        (f"greenhouse:{b['token']}", "greenhouse", b["token"], b["company"])
        for b in config["greenhouse"]["boards"]
    ]
    rows += [
        (f"lever:{b['token']}", "lever", b["token"], b["company"])
        for b in config["lever"]["companies"]
    ]
    repo = config["github_list"]["repo"]
    rows.append((f"github:{repo}", "github", repo, repo))
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO easyapply.source_feeds (feed_key, source, feed_token, display_name)
            VALUES (%s, %s, %s, %s) ON CONFLICT (feed_key)
            DO UPDATE SET display_name = EXCLUDED.display_name
        """,
            rows,
        )


def _read_batch(data_dir: Path, run_id: str, config: dict) -> dict:
    if not run_id or Path(run_id).name != run_id or run_id in (".", ".."):
        raise ValueError("run_id must be a single directory name")
    paths = [
        data_dir / "staging" / run_id / "manifest.json",
        data_dir / "processed" / run_id / "postings.parquet",
        data_dir / "processed" / run_id / "review_candidates.parquet",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    manifest = json.loads(paths[0].read_text())
    if manifest["run_id"] != run_id:
        raise ValueError("Manifest run_id does not match the selected batch")
    observed_at = utc(manifest["fetched_at"])
    postings, candidates = (pl.read_parquet(p) for p in paths[1:])
    missing = (REQUIRED | set(VERSION_FIELDS)) - set(postings.columns)
    if missing or CANDIDATE_FIELDS - set(candidates.columns):
        raise ValueError(f"Batch schema is missing required fields: {sorted(missing)}")
    collections = {}
    for c in manifest["collections"]:
        key = feed_key(c["source"], c["company_token"], config)
        if key in collections:
            raise ValueError("Manifest contains duplicate source collections")
        if not isinstance(c["is_complete"], bool):
            raise ValueError("is_complete must be a boolean")
        if c["received_count"] < 0:
            raise ValueError("received_count cannot be negative")
        if c["is_complete"] and c.get("expected_count") is not None:
            if c["expected_count"] != c["received_count"]:
                raise ValueError("Complete collection has mismatched expected and received counts")
        collections[key] = c
    if sum(c["received_count"] for c in collections.values()) != manifest["total_records"]:
        raise ValueError("Manifest collection counts do not match total_records")
    if manifest["all_complete"] != all(c["is_complete"] for c in collections.values()):
        raise ValueError("Manifest all_complete disagrees with collection results")
    seen = set()
    counts = Counter()
    rows = postings.to_dicts()
    for row in rows:
        identity = (row["source"], row["external_id"])
        if not all(isinstance(x, str) and x.strip() for x in identity) or identity in seen:
            raise ValueError("Source posting identities must be nonempty and unique per batch")
        seen.add(identity)
        key = feed_key(row["source"], row["company_token"], config)
        if key not in collections:
            raise ValueError("Posting has no corresponding manifest collection")
        counts[key] += 1
        if row["run_id"] != run_id or utc(row["fetched_at"]) != observed_at:
            raise ValueError("Posting collection identity or timestamp disagrees with manifest")
        if row["snapshot_path"] != collections[key]["snapshot_path"]:
            raise ValueError("Posting lineage disagrees with manifest snapshot_path")
        if hashlib.sha1(row["opportunity_key"].encode()).hexdigest()[:16] != row["opportunity_id"]:
            raise ValueError("Opportunity ID disagrees with the upstream identity rule")
        row["extra"] = json.loads(row["extra"]) if isinstance(row["extra"], str) else row["extra"]
        if not isinstance(row["extra"], dict):
            raise ValueError("extra must contain a JSON object")
        if row["source"] == "github" and row["extra"].get("repo") != config["github_list"]["repo"]:
            raise ValueError("GitHub posting repository disagrees with configured feed")
        row["feed_key"] = key
    if any(counts[k] > c["received_count"] for k, c in collections.items()):
        raise ValueError("Processed posting counts exceed collected record counts")
    for c in candidates.to_dicts():
        if (c["left_source"], c["left_external_id"]) not in seen or (
            c["right_source"],
            c["right_external_id"],
        ) not in seen:
            raise ValueError("Review candidate refers to a posting outside the batch")
    return {
        "manifest": manifest,
        "rows": rows,
        "candidates": candidates.to_dicts(),
        "collections": collections,
        "batch_hash": digest.hexdigest(),
        "fetched_at": observed_at,
    }


def state_hash(row: dict) -> str:
    # Preserve the upstream content hash and additionally track searchable metadata.
    fields = {
        k: row.get(k)
        for k in (
            "content_hash",
            "company_normalized",
            "title_normalized",
            "location_normalized",
            "apply_url_normalized",
            "ats_job_key",
            "is_target_role",
            "posted_at",
            "extra",
        )
    }
    fields["extra"] = _version_extra(row)
    body = json.dumps(fields, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(body.encode()).hexdigest()


def _version_extra(row: dict) -> dict:
    return {
        k: v
        for k, v in row["extra"].items()
        if not (row["source"] == "github" and k == "commit_sha")
    }


def load_run(
    data_dir: str | Path, run_id: str, *, dsn: str | None = None, config: dict | None = None
) -> dict:
    """Load a transformed batch. Airflow supplies the exact run_id, never 'latest'."""
    config = config or load_config()
    batch = _read_batch(Path(data_dir), run_id, config)
    with connect(dsn) as conn:
        conn.autocommit = True
        conn.execute("SELECT pg_advisory_lock(70627002)")
        try:
            return _load_locked(conn, run_id, batch, config)
        finally:
            conn.execute("SELECT pg_advisory_unlock(70627002)")


def _load_locked(conn, run_id: str, batch: dict, config: dict) -> dict:
    existing = conn.execute(
        "SELECT * FROM easyapply.pipeline_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    if existing:
        if existing["batch_hash"] != batch["batch_hash"]:
            raise ValueError("Previously registered run_id has different input bytes")
        if existing["status"] == "loaded":
            return {
                "run_id": run_id,
                "status": "already_loaded",
                "postings": existing["posting_count"],
                "review_candidates": existing["review_count"],
            }
    with conn.transaction():
        register_feeds(conn, config)
        conn.execute(
            """
            INSERT INTO easyapply.pipeline_runs
                (run_id, fetched_at, batch_hash, status, input_records)
            VALUES (%s, %s, %s, 'loading', %s)
            ON CONFLICT (run_id) DO UPDATE SET status = 'loading', error_type = NULL,
                finished_at = NULL, started_at = CURRENT_TIMESTAMP
        """,
            (run_id, batch["fetched_at"], batch["batch_hash"], batch["manifest"]["total_records"]),
        )
        for key, c in batch["collections"].items():
            conn.execute(
                """
                INSERT INTO easyapply.source_collections
                    (run_id, feed_key, is_complete, http_status, expected_count, received_count,
                     snapshot_path, checksum, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, feed_key) DO NOTHING
            """,
                (
                    run_id,
                    key,
                    c["is_complete"],
                    c.get("http_status"),
                    c.get("expected_count"),
                    c["received_count"],
                    c.get("snapshot_path"),
                    c.get("checksum"),
                    c.get("error"),
                ),
            )
    try:
        with conn.transaction():
            identities = {}
            for row in batch["rows"]:
                identities[(row["source"], row["external_id"])] = _load_posting(conn, run_id, row)
            _load_reviews(conn, run_id, batch, identities)
            result = {
                "run_id": run_id,
                "status": "loaded",
                "postings": len(batch["rows"]),
                "review_candidates": len(batch["candidates"]),
            }
            conn.execute(
                """
                UPDATE easyapply.pipeline_runs
                SET status = 'loaded', finished_at = CURRENT_TIMESTAMP,
                    posting_count = %s, review_count = %s WHERE run_id = %s
            """,
                (result["postings"], result["review_candidates"], run_id),
            )
        return result
    except Exception as exc:
        conn.execute(
            """
            UPDATE easyapply.pipeline_runs SET status = 'failed', finished_at = CURRENT_TIMESTAMP,
                error_type = %s WHERE run_id = %s
        """,
            (type(exc).__name__, run_id),
        )
        raise


def _load_posting(conn, run_id: str, row: dict) -> int:
    conn.execute(
        """
        INSERT INTO easyapply.opportunities (opportunity_key, opportunity_id)
        VALUES (%s, %s) ON CONFLICT (opportunity_key) DO NOTHING
    """,
        (row["opportunity_key"], row["opportunity_id"]),
    )
    posting = conn.execute(
        """
        INSERT INTO easyapply.postings (source, external_id, feed_key, opportunity_key)
        VALUES (%s, %s, %s, %s) ON CONFLICT (source, external_id)
        DO NOTHING RETURNING *
    """,
        (row["source"], row["external_id"], row["feed_key"], row["opportunity_key"]),
    ).fetchone()
    if posting is None:
        posting = conn.execute(
            "SELECT * FROM easyapply.postings WHERE source = %s AND external_id = %s",
            (row["source"], row["external_id"]),
        ).fetchone()
    if (posting["feed_key"], posting["opportunity_key"]) != (
        row["feed_key"],
        row["opportunity_key"],
    ):
        raise ValueError(
            "Existing source posting changed feed or opportunity identity; review upstream mapping"
        )
    version = conn.execute(
        """
        INSERT INTO easyapply.posting_versions (
            posting_id, state_hash, content_hash, company, company_normalized,
            title, title_normalized,
            location, location_normalized, apply_url, apply_url_normalized, ats_job_key,
            description_text, description_html, is_target_role, posted_at, updated_at, extra
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (posting_id, state_hash) DO NOTHING
        RETURNING version_id
    """,
        (
            posting["posting_id"],
            state_hash(row),
            *(Jsonb(_version_extra(row)) if k == "extra" else row.get(k) for k in VERSION_FIELDS),
        ),
    ).fetchone()
    new_version = version is not None
    if version is None:
        version = conn.execute(
            "SELECT version_id FROM easyapply.posting_versions "
            "WHERE posting_id = %s AND state_hash = %s",
            (posting["posting_id"], state_hash(row)),
        ).fetchone()
    conn.execute(
        """
        INSERT INTO easyapply.posting_observations
            (run_id, posting_id, feed_key, version_id, snapshot_path,
             source_updated_at, source_extra, match_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            run_id,
            posting["posting_id"],
            row["feed_key"],
            version["version_id"],
            row["snapshot_path"],
            row.get("updated_at"),
            Jsonb(row["extra"]),
            row["match_method"],
        ),
    )
    evidence_rows = (
        extract_evidence(row.get("description_text"), row["extra"]) if new_version else []
    )
    for evidence in evidence_rows:
        conn.execute(
            """
            INSERT INTO easyapply.requirement_evidence
                (version_id, attribute, value, excerpt, evidence_field, method)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
        """,
            (
                version["version_id"],
                evidence["attribute"],
                evidence["value"],
                evidence["excerpt"],
                evidence["evidence_field"],
                evidence["method"],
            ),
        )
    return posting["posting_id"]


def _load_reviews(conn, run_id: str, batch: dict, identities: dict) -> None:
    feed_by_posting = {(r["source"], r["external_id"]): r["feed_key"] for r in batch["rows"]}
    for c in batch["candidates"]:
        pair = [
            (c["left_source"], c["left_external_id"]),
            (c["right_source"], c["right_external_id"]),
        ]
        if not all(batch["collections"][feed_by_posting[p]]["is_complete"] for p in pair):
            continue
        left, right = sorted(identities[p] for p in pair)
        conn.execute(
            """
            INSERT INTO easyapply.review_candidates
                (left_posting_id, right_posting_id, first_seen_run, last_seen_run,
                 title_score, location_overlap)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (left_posting_id, right_posting_id) DO UPDATE
            SET last_seen_run = EXCLUDED.last_seen_run, title_score = EXCLUDED.title_score,
                location_overlap = EXCLUDED.location_overlap
            WHERE (SELECT fetched_at FROM easyapply.pipeline_runs
                   WHERE run_id = EXCLUDED.last_seen_run)
                >= (SELECT fetched_at FROM easyapply.pipeline_runs
                    WHERE run_id = review_candidates.last_seen_run)
        """,
            (left, right, run_id, run_id, c["title_score"], c["location_overlap"]),
        )
