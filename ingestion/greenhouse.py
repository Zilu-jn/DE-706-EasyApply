"""Greenhouse Job Board API adapter.

Docs: https://developers.greenhouse.io/job-board.html
One request returns every published job on a board, with `meta.total`
telling us how many to expect.
"""

from __future__ import annotations

import html
import json
from typing import Any

import requests

from ingestion.base import (
    CollectionResult,
    FetchError,
    RawRecord,
    RunContext,
    http_get_json,
    parse_datetime,
    save_snapshot,
)

SOURCE = "greenhouse"


def parse_jobs(
    payload: dict[str, Any], *, token: str, company: str, ctx: RunContext, snapshot_path: str
) -> list[RawRecord]:
    """Turn a Greenhouse /jobs?content=true response into RawRecords. Pure function."""
    records = []
    for job in payload.get("jobs", []):
        location = (job.get("location") or {}).get("name")
        extra = {
            "requisition_id": job.get("requisition_id"),
            "departments": [d.get("name") for d in job.get("departments") or []],
            "offices": [o.get("name") for o in job.get("offices") or []],
        }
        records.append(
            RawRecord(
                source=SOURCE,
                external_id=str(job["id"]),
                company_token=token,
                company=company,
                title=(job.get("title") or "").strip(),
                location=location,
                apply_url=job.get("absolute_url"),
                # Greenhouse returns the description HTML-escaped (&lt;p&gt;...)
                description_html=html.unescape(job.get("content") or "") or None,
                posted_at=parse_datetime(job.get("first_published")),
                updated_at=parse_datetime(job.get("updated_at")),
                fetched_at=ctx.fetched_at,
                run_id=ctx.run_id,
                snapshot_path=snapshot_path,
                extra=json.dumps(extra, ensure_ascii=False),
            )
        )
    return records


def check_complete(payload: dict[str, Any]) -> tuple[bool, int | None]:
    """A board is complete when we received as many jobs as meta.total says."""
    jobs = payload.get("jobs")
    total = (payload.get("meta") or {}).get("total")
    if not isinstance(jobs, list):
        return False, total
    if total is None:
        return True, None
    return len(jobs) == total, total


def collect(
    board: dict[str, str],
    ctx: RunContext,
    http_cfg: dict[str, Any],
    base_url: str,
    session: requests.Session | None = None,
) -> CollectionResult:
    token, company = board["token"], board["company"]
    result = CollectionResult(source=SOURCE, company_token=token)
    url = f"{base_url}/{token}/jobs"
    try:
        payload, status = http_get_json(
            url,
            session=session,
            params={"content": "true"},
            headers={"User-Agent": http_cfg["user_agent"]},
            timeout=http_cfg["timeout_seconds"],
            max_retries=http_cfg["max_retries"],
            backoff=http_cfg["backoff_seconds"],
        )
    except FetchError as e:
        result.error, result.http_status = str(e), e.status
        return result

    result.http_status = status
    result.snapshot_path, result.checksum = save_snapshot(payload, SOURCE, token, ctx)
    result.is_complete, result.expected_count = check_complete(payload)
    result.records = parse_jobs(
        payload, token=token, company=company, ctx=ctx, snapshot_path=result.snapshot_path
    )
    if not result.is_complete:
        result.error = f"Expected {result.expected_count} jobs, received {len(result.records)}"
    return result
