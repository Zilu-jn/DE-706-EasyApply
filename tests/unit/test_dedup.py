from datetime import UTC, datetime

import polars as pl

from transform.dedup import assign_opportunities, dedup_within_source, find_review_candidates


def frame(rows):
    base = {
        "source": None,
        "external_id": None,
        "company_normalized": "acme",
        "title": "Data Intern",
        "title_normalized": "data intern",
        "location_normalized": "new york, ny",
        "ats_job_key": None,
        "apply_url_normalized": None,
        "fetched_at": datetime(2026, 11, 19, tzinfo=UTC),
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def test_within_source_keeps_latest_observation():
    df = frame(
        [
            {
                "source": "lever",
                "external_id": "1",
                "title": "old",
                "fetched_at": datetime(2026, 11, 1, tzinfo=UTC),
            },
            {
                "source": "lever",
                "external_id": "1",
                "title": "new",
                "fetched_at": datetime(2026, 11, 2, tzinfo=UTC),
            },
        ]
    )
    out = dedup_within_source(df)
    assert out.height == 1
    assert out["title"][0] == "new"


def test_shared_ats_id_links_across_sources():
    df = frame(
        [
            {
                "source": "greenhouse",
                "external_id": "123",
                "ats_job_key": "greenhouse:123",
                "apply_url_normalized": "https://job-boards.greenhouse.io/acme/jobs/123",
            },
            {
                "source": "github",
                "external_id": "uuid-a",
                "ats_job_key": "greenhouse:123",
                "apply_url_normalized": "https://job-boards.greenhouse.io/acme/jobs/123",
                "title_normalized": "data co-op",
            },  # different title on purpose
        ]
    )
    out = assign_opportunities(df)
    assert out["opportunity_id"].n_unique() == 1
    assert set(out["match_method"]) == {"ats_id"}
    assert set(out["n_sources"]) == {2}


def test_url_fallback_when_no_ats_id():
    url = "https://careers.acme.com/jobs/42"
    df = frame(
        [
            {"source": "github", "external_id": "a", "apply_url_normalized": url},
            {"source": "greenhouse", "external_id": "b", "apply_url_normalized": url},
        ]
    )
    out = assign_opportunities(df)
    assert out["opportunity_id"].n_unique() == 1
    assert set(out["match_method"]) == {"url"}


def test_singletons_get_their_own_opportunity_and_no_match_method():
    df = frame(
        [
            {"source": "lever", "external_id": "x", "ats_job_key": "lever:x"},
            {"source": "lever", "external_id": "y"},
        ]
    )
    out = assign_opportunities(df)
    assert out["opportunity_id"].n_unique() == 2
    assert set(out["match_method"]) == {"none"}
    assert "lever:y" in out["opportunity_key"].to_list()


def test_opportunity_id_is_stable_across_runs():
    df = frame([{"source": "lever", "external_id": "x", "ats_job_key": "lever:x"}])
    assert (
        assign_opportunities(df)["opportunity_id"][0]
        == assign_opportunities(df)["opportunity_id"][0]
    )


def test_review_candidates_cross_source_similar_titles_only():
    df = assign_opportunities(
        frame(
            [
                {
                    "source": "lever",
                    "external_id": "1",
                    "ats_job_key": "lever:1",
                    "title_normalized": "ml intern - perception",
                },
                {
                    "source": "github",
                    "external_id": "g",
                    "ats_job_key": "lever:2",
                    "title_normalized": "ml intern, perception",
                },
                # same source, same title: separate requisitions, not a candidate
                {
                    "source": "lever",
                    "external_id": "3",
                    "ats_job_key": "lever:3",
                    "title_normalized": "ml intern - perception",
                },
                # different company: never compared
                {
                    "source": "github",
                    "external_id": "h",
                    "company_normalized": "other",
                    "title_normalized": "ml intern - perception",
                },
            ]
        )
    )
    cands = find_review_candidates(df)
    pairs = {
        tuple(sorted([r["left_external_id"], r["right_external_id"]])) for r in cands.to_dicts()
    }
    assert pairs == {("1", "g"), ("3", "g")}
    assert set(cands["status"]) == {"pending"}


def test_review_candidates_skip_already_linked_pairs():
    df = assign_opportunities(
        frame(
            [
                {"source": "greenhouse", "external_id": "1", "ats_job_key": "greenhouse:1"},
                {"source": "github", "external_id": "g", "ats_job_key": "greenhouse:1"},
            ]
        )
    )
    assert find_review_candidates(df).height == 0
