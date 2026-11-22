"""Ingestion -> cleaning -> dedup on saved real responses. No network."""

import polars as pl
import pytest

from ingestion import github_list, greenhouse, lever
from ingestion.run import records_to_frame
from tests.conftest import TARGET_KEYWORDS, load_fixture
from transform.run import transform


@pytest.fixture
def raw_frame(ctx):
    records = []
    records += greenhouse.parse_jobs(
        load_fixture("greenhouse_lilasciences.json"),
        token="lilasciences",
        company="Lila Sciences",
        ctx=ctx,
        snapshot_path="gh.json",
    )
    records += lever.parse_postings(
        load_fixture("lever_tri.json"),
        token="tri",
        company="Toyota Research Institute",
        ctx=ctx,
        snapshot_path="lv.json",
    )
    records += github_list.parse_listings(
        load_fixture("github_listings.json"),
        categories=["AI/ML/Data", "Data Science, AI & Machine Learning"],
        active_only=True,
        repo="SimplifyJobs/Summer2027-Internships",
        commit_sha="abc",
        ctx=ctx,
        snapshot_path="gl.json",
    )
    return records_to_frame(records)


def test_end_to_end_on_fixtures(raw_frame):
    postings, candidates = transform(raw_frame, TARGET_KEYWORDS)

    # Every input record survives (no silent drops) and keeps its lineage
    assert postings.height == raw_frame.height
    assert postings["snapshot_path"].null_count() == 0

    # The two Greenhouse jobs that are also in the GitHub list are linked by job ID
    cross = postings.filter(pl.col("n_sources") > 1)
    assert cross["opportunity_id"].n_unique() == 2
    assert set(cross["match_method"]) == {"ats_id"}

    # The TRI pair with the same title but different Lever IDs is a candidate, not a merge
    assert candidates.height >= 1
    assert set(candidates["status"]) == {"pending"}

    # Cleaned description is present for Greenhouse/Lever, absent for GitHub
    has_desc = postings.filter(pl.col("source") != "github")["description_text"]
    assert has_desc.null_count() == 0
    assert (
        postings.filter(pl.col("source") == "github")["description_text"].null_count()
        == postings.filter(pl.col("source") == "github").height
    )


def test_replaying_same_batch_gives_identical_output(raw_frame):
    first, first_c = transform(raw_frame, TARGET_KEYWORDS)
    second, second_c = transform(raw_frame, TARGET_KEYWORDS)
    assert first.equals(second)
    assert first_c.equals(second_c)


def test_feeding_duplicated_batch_does_not_create_duplicates(raw_frame):
    once, _ = transform(raw_frame, TARGET_KEYWORDS)
    twice, _ = transform(pl.concat([raw_frame, raw_frame]), TARGET_KEYWORDS)
    assert twice.height == once.height
    assert twice["opportunity_id"].n_unique() == once["opportunity_id"].n_unique()
