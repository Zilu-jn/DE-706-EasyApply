import hashlib
import json
from pathlib import Path

import requests

from ingestion import github_list, greenhouse, lever
from tests.conftest import HTTP_CFG, FakeResponse, FakeSession, load_fixture

GH_BASE = "https://boards-api.greenhouse.io/v1/boards"
LV_BASE = "https://api.lever.co/v0/postings"


# ---------------------------------------------------------------- Greenhouse


def test_greenhouse_parse_fixture(ctx):
    payload = load_fixture("greenhouse_lilasciences.json")
    records = greenhouse.parse_jobs(
        payload, token="lilasciences", company="Lila Sciences", ctx=ctx, snapshot_path="x.json"
    )
    assert len(records) == len(payload["jobs"]) == 3
    r = records[0]
    assert r.source == "greenhouse"
    assert r.external_id == str(payload["jobs"][0]["id"])
    assert r.run_id == "test-run"
    assert r.description_html.lstrip().startswith("<")  # HTML was unescaped
    assert "&lt;" not in r.description_html
    assert r.posted_at is not None and r.posted_at.tzinfo is not None


def test_greenhouse_completeness_check():
    assert greenhouse.check_complete({"jobs": [{}, {}], "meta": {"total": 2}}) == (True, 2)
    assert greenhouse.check_complete({"jobs": [{}], "meta": {"total": 2}}) == (False, 2)
    assert greenhouse.check_complete({"meta": {"total": 2}}) == (False, 2)


def test_greenhouse_collect_saves_snapshot_with_checksum(ctx):
    payload = load_fixture("greenhouse_lilasciences.json")
    session = FakeSession([FakeResponse(200, payload)])
    result = greenhouse.collect(
        {"token": "lilasciences", "company": "Lila Sciences"}, ctx, HTTP_CFG, GH_BASE, session
    )
    assert result.is_complete and result.error is None
    assert len(result.records) == 3
    snap = Path(result.snapshot_path)
    assert snap.exists()
    assert hashlib.sha256(snap.read_bytes()).hexdigest() == result.checksum
    assert all(r.snapshot_path == result.snapshot_path for r in result.records)


def test_greenhouse_collect_failure_is_incomplete_and_writes_nothing(ctx):
    session = FakeSession([FakeResponse(404, None)])
    result = greenhouse.collect(
        {"token": "nope", "company": "Nope"}, ctx, HTTP_CFG, GH_BASE, session
    )
    assert not result.is_complete
    assert result.records == []
    assert result.snapshot_path is None
    assert "404" in result.error


def test_greenhouse_collect_retries_then_succeeds(ctx):
    payload = load_fixture("greenhouse_lilasciences.json")
    session = FakeSession([requests.Timeout("slow"), FakeResponse(200, payload)])
    result = greenhouse.collect(
        {"token": "lilasciences", "company": "Lila Sciences"}, ctx, HTTP_CFG, GH_BASE, session
    )
    assert result.is_complete
    assert len(session.calls) == 2


def test_greenhouse_short_response_is_incomplete(ctx):
    payload = load_fixture("greenhouse_lilasciences.json")
    payload["meta"]["total"] = 10
    session = FakeSession([FakeResponse(200, payload)])
    result = greenhouse.collect(
        {"token": "lilasciences", "company": "Lila Sciences"}, ctx, HTTP_CFG, GH_BASE, session
    )
    assert not result.is_complete
    assert "Expected 10" in result.error


# ---------------------------------------------------------------- Lever


def test_lever_parse_fixture_builds_full_description(ctx):
    postings = load_fixture("lever_tri.json")
    records = lever.parse_postings(
        postings, token="tri", company="Toyota Research Institute", ctx=ctx, snapshot_path="x"
    )
    assert len(records) == len(postings)
    for p, r in zip(postings, records, strict=True):
        assert r.external_id == p["id"]
        for section in p.get("lists") or []:
            assert section["text"] in r.description_html
    assert json.loads(records[0].extra)["commitment"] is not None


def test_lever_collect_paginates_until_short_page(ctx):
    postings = load_fixture("lever_tri.json")
    # page_size=1: one full page per posting, then an empty page signals the end
    pages = [FakeResponse(200, [p]) for p in postings] + [FakeResponse(200, [])]
    session = FakeSession(pages)
    result = lever.collect(
        {"token": "tri", "company": "TRI"}, ctx, HTTP_CFG, LV_BASE, page_size=1, session=session
    )
    assert result.is_complete
    assert len(result.records) == len(postings)
    assert [c["params"]["skip"] for c in session.calls] == list(range(len(postings) + 1))


def test_lever_collect_failure_mid_pagination_is_incomplete(ctx):
    postings = load_fixture("lever_tri.json")
    session = FakeSession(
        [FakeResponse(200, postings[:2]), FakeResponse(500, None), FakeResponse(500, None)]
    )
    result = lever.collect(
        {"token": "tri", "company": "TRI"}, ctx, HTTP_CFG, LV_BASE, page_size=2, session=session
    )
    assert not result.is_complete
    assert len(result.records) == 2  # partial data kept for debugging, but flagged
    assert result.error


# ---------------------------------------------------------------- GitHub list


def test_github_parse_filters_categories_and_inactive(ctx):
    listings = load_fixture("github_listings.json")
    extra_item = dict(listings[0], id="inactive-1", active=False)
    other_cat = dict(listings[0], id="hw-1", category="Hardware")
    records = github_list.parse_listings(
        listings + [extra_item, other_cat],
        categories=["AI/ML/Data", "Data Science, AI & Machine Learning"],
        active_only=True,
        repo="SimplifyJobs/Summer2027-Internships",
        commit_sha="abc123",
        ctx=ctx,
        snapshot_path="x",
    )
    ids = {r.external_id for r in records}
    assert "inactive-1" not in ids and "hw-1" not in ids
    assert len(records) == len(listings)
    extra = json.loads(records[0].extra)
    assert extra["commit_sha"] == "abc123"
    assert "sponsorship" in extra


def test_github_collect_pins_commit_sha(ctx):
    listings = load_fixture("github_listings.json")
    session = FakeSession([FakeResponse(200, {"sha": "deadbeef" * 5}), FakeResponse(200, listings)])
    cfg = {
        "repo": "SimplifyJobs/Summer2027-Internships",
        "branch": "dev",
        "path": ".github/scripts/listings.json",
        "categories": ["AI/ML/Data"],
    }
    result = github_list.collect(cfg, ctx, HTTP_CFG, session=session)
    assert result.is_complete
    assert ("deadbeef" * 5) in session.calls[1]["url"]
    assert "@deadbeefdead" in result.snapshot_path
