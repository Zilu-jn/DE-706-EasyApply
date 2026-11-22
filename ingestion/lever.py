"""Lever Postings API adapter.

Docs: https://github.com/lever/postings-api
Paginated with skip/limit. There is no total count, so a collection is complete
only if every page came back successfully and the last page was short.
"""

from __future__ import annotations

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

SOURCE = "lever"


def build_description_html(posting: dict[str, Any]) -> str | None:
    """Lever splits the description into an intro, titled lists, and a closing section."""
    parts = []
    if posting.get("description"):
        parts.append(posting["description"])
    for section in posting.get("lists") or []:
        title = section.get("text") or ""
        parts.append(f"<h3>{title}</h3><ul>{section.get('content') or ''}</ul>")
    if posting.get("additional"):
        parts.append(posting["additional"])
    return "\n".join(parts) or None


def parse_postings(
    postings: list[dict[str, Any]],
    *,
    token: str,
    company: str,
    ctx: RunContext,
    snapshot_path: str,
) -> list[RawRecord]:
    """Turn a list of Lever postings into RawRecords. Pure function."""
    records = []
    for p in postings:
        cats = p.get("categories") or {}
        locations = cats.get("allLocations") or ([cats["location"]] if cats.get("location") else [])
        extra = {
            "commitment": cats.get("commitment"),
            "team": cats.get("team"),
            "department": cats.get("department"),
            "workplace_type": p.get("workplaceType"),
            "country": p.get("country"),
            "salary_range": p.get("salaryRange"),
        }
        records.append(
            RawRecord(
                source=SOURCE,
                external_id=str(p["id"]),
                company_token=token,
                company=company,
                title=(p.get("text") or "").strip(),
                location=" / ".join(locations) or None,
                apply_url=p.get("hostedUrl") or p.get("applyUrl"),
                description_html=build_description_html(p),
                posted_at=parse_datetime(p.get("createdAt")),
                updated_at=None,  # Lever's public API has no updated timestamp
                fetched_at=ctx.fetched_at,
                run_id=ctx.run_id,
                snapshot_path=snapshot_path,
                extra=json.dumps(extra, ensure_ascii=False),
            )
        )
    return records


def collect(
    company_cfg: dict[str, str],
    ctx: RunContext,
    http_cfg: dict[str, Any],
    base_url: str,
    page_size: int = 100,
    session: requests.Session | None = None,
    max_pages: int = 50,
) -> CollectionResult:
    token, company = company_cfg["token"], company_cfg["company"]
    result = CollectionResult(source=SOURCE, company_token=token)
    url = f"{base_url}/{token}"
    postings: list[dict[str, Any]] = []
    finished = False
    try:
        for page in range(max_pages):
            data, status = http_get_json(
                url,
                session=session,
                params={"mode": "json", "skip": page * page_size, "limit": page_size},
                headers={"User-Agent": http_cfg["user_agent"]},
                timeout=http_cfg["timeout_seconds"],
                max_retries=http_cfg["max_retries"],
                backoff=http_cfg["backoff_seconds"],
            )
            result.http_status = status
            if not isinstance(data, list):
                raise FetchError(f"Expected a list from {url}, got {type(data).__name__}", status)
            postings.extend(data)
            if len(data) < page_size:
                finished = True
                break
    except FetchError as e:
        # Keep whatever pages we got for debugging, but mark the collection incomplete
        result.error, result.http_status = str(e), e.status

    if postings or finished:
        result.snapshot_path, result.checksum = save_snapshot(postings, SOURCE, token, ctx)
        result.records = parse_postings(
            postings, token=token, company=company, ctx=ctx, snapshot_path=result.snapshot_path
        )
    result.is_complete = finished and result.error is None
    if not finished and result.error is None:
        result.error = f"Stopped after {max_pages} pages without reaching the end"
    return result
