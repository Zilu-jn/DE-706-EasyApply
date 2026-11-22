"""Community internship list adapter (SimplifyJobs).

We pin each run to an exact commit SHA so the snapshot is reproducible:
first ask the GitHub API for the branch's latest commit, then download
listings.json at that commit. These records have no job description; their
value is discovery plus links that often point straight to Greenhouse/Lever.
"""

from __future__ import annotations

import json
import os
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

SOURCE = "github"


def parse_listings(
    listings: list[dict[str, Any]],
    *,
    categories: list[str],
    active_only: bool,
    repo: str,
    commit_sha: str,
    ctx: RunContext,
    snapshot_path: str,
) -> list[RawRecord]:
    """Filter listings to our categories and turn them into RawRecords. Pure function."""
    wanted = set(categories)
    records = []
    for item in listings:
        if item.get("category") not in wanted:
            continue
        if not item.get("is_visible", True):
            continue
        if active_only and not item.get("active"):
            continue
        extra = {
            "repo": repo,
            "commit_sha": commit_sha,
            "category": item.get("category"),
            "terms": item.get("terms"),
            "sponsorship": item.get("sponsorship"),
            "degrees": item.get("degrees"),
            "active": item.get("active"),
            "list_source": item.get("source"),
        }
        records.append(
            RawRecord(
                source=SOURCE,
                external_id=str(item["id"]),
                company_token=None,
                company=(item.get("company_name") or "").strip(),
                title=(item.get("title") or "").strip(),
                location=" / ".join(item.get("locations") or []) or None,
                apply_url=item.get("url"),
                description_html=None,
                posted_at=parse_datetime(item.get("date_posted")),
                updated_at=parse_datetime(item.get("date_updated")),
                fetched_at=ctx.fetched_at,
                run_id=ctx.run_id,
                snapshot_path=snapshot_path,
                extra=json.dumps(extra, ensure_ascii=False),
            )
        )
    return records


def collect(
    cfg: dict[str, Any],
    ctx: RunContext,
    http_cfg: dict[str, Any],
    session: requests.Session | None = None,
) -> CollectionResult:
    repo, branch, path = cfg["repo"], cfg["branch"], cfg["path"]
    result = CollectionResult(source=SOURCE, company_token=None)
    headers = {"User-Agent": http_cfg["user_agent"]}
    api_headers = dict(headers, Accept="application/vnd.github+json")
    if os.getenv("GITHUB_TOKEN"):
        api_headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    retry = {
        "timeout": http_cfg["timeout_seconds"],
        "max_retries": http_cfg["max_retries"],
        "backoff": http_cfg["backoff_seconds"],
    }
    try:
        commit, _ = http_get_json(
            f"https://api.github.com/repos/{repo}/commits/{branch}",
            session=session,
            headers=api_headers,
            **retry,
        )
        sha = commit["sha"]
        listings, status = http_get_json(
            f"https://raw.githubusercontent.com/{repo}/{sha}/{path}",
            session=session,
            headers=headers,
            **retry,
        )
    except (FetchError, KeyError) as e:
        result.error = str(e)
        result.http_status = getattr(e, "status", None)
        return result

    result.http_status = status
    if not isinstance(listings, list) or not listings:
        result.error = "listings.json was empty or not a list"
        return result

    key = f"{repo.replace('/', '__')}@{sha[:12]}"
    result.snapshot_path, result.checksum = save_snapshot(listings, SOURCE, key, ctx)
    result.records = parse_listings(
        listings,
        categories=cfg["categories"],
        active_only=cfg.get("active_only", True),
        repo=repo,
        commit_sha=sha,
        ctx=ctx,
        snapshot_path=result.snapshot_path,
    )
    result.expected_count = len(result.records)
    result.is_complete = True
    return result
