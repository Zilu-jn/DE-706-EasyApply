"""Identity resolution: which records are the same opportunity?

Rules (docs/data_contract.md):
1. Within a source, (source, external_id) is unique; keep the latest observation.
2. Across sources, records sharing an ATS job ID or a normalized URL are the
   same opportunity. These are confident matches.
3. Same company + very similar title, but no shared ID or URL, is only a
   *candidate*. Candidates go to a review queue and are never merged here.
"""

from __future__ import annotations

import hashlib

import polars as pl
from rapidfuzz import fuzz


def dedup_within_source(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort("fetched_at", descending=True, nulls_last=True).unique(
        subset=["source", "external_id"], keep="first", maintain_order=True
    )


def _opportunity_id(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def assign_opportunities(df: pl.DataFrame) -> pl.DataFrame:
    """Add opportunity_key, opportunity_id, match_method, and group stats."""
    keyed = df.with_columns(
        pl.coalesce(
            pl.col("ats_job_key"),
            pl.col("apply_url_normalized"),
            pl.concat_str([pl.col("source"), pl.lit(":"), pl.col("external_id")]),
        ).alias("opportunity_key"),
        pl.when(pl.col("ats_job_key").is_not_null())
        .then(pl.lit("ats_id"))
        .when(pl.col("apply_url_normalized").is_not_null())
        .then(pl.lit("url"))
        .otherwise(pl.lit("none"))
        .alias("_key_kind"),
    )
    keyed = keyed.with_columns(
        pl.len().over("opportunity_key").alias("group_size"),
        pl.col("source").n_unique().over("opportunity_key").alias("n_sources"),
    )
    keyed = keyed.with_columns(
        pl.when(pl.col("group_size") > 1)
        .then(pl.col("_key_kind"))
        .otherwise(pl.lit("none"))
        .alias("match_method"),
        pl.col("opportunity_key")
        .map_elements(_opportunity_id, return_dtype=pl.Utf8)
        .alias("opportunity_id"),
    )
    return keyed.drop("_key_kind")


def find_review_candidates(df: pl.DataFrame, *, title_threshold: float = 90.0) -> pl.DataFrame:
    """Cross-source pairs that look alike but share no ID or URL.

    Blocked by company so we only compare postings from the same employer.
    Same-source pairs are skipped on purpose: two Greenhouse jobs with the same
    title are usually separate requisitions (e.g. different offices).
    """
    cols = [
        "source",
        "external_id",
        "opportunity_key",
        "company_normalized",
        "title",
        "title_normalized",
        "location_normalized",
    ]
    reps = df.filter(
        pl.col("company_normalized").is_not_null() & pl.col("title_normalized").is_not_null()
    ).select(cols)
    rows = []
    for (_company,), group in reps.group_by(["company_normalized"]):
        if group["source"].n_unique() < 2:
            continue
        recs = group.to_dicts()
        for i, a in enumerate(recs):
            for b in recs[i + 1 :]:
                if a["source"] == b["source"] or a["opportunity_key"] == b["opportunity_key"]:
                    continue
                score = fuzz.token_sort_ratio(a["title_normalized"], b["title_normalized"])
                if score < title_threshold:
                    continue
                loc_a, loc_b = a["location_normalized"], b["location_normalized"]
                loc_overlap = (
                    loc_a is None
                    or loc_b is None
                    or bool(set(loc_a.split(" / ")) & set(loc_b.split(" / ")))
                )
                rows.append(
                    {
                        "company_normalized": a["company_normalized"],
                        "left_source": a["source"],
                        "left_external_id": a["external_id"],
                        "left_title": a["title"],
                        "left_location": loc_a,
                        "right_source": b["source"],
                        "right_external_id": b["external_id"],
                        "right_title": b["title"],
                        "right_location": loc_b,
                        "title_score": float(score),
                        "location_overlap": loc_overlap,
                        "status": "pending",
                    }
                )
    schema = {
        "company_normalized": pl.Utf8,
        "left_source": pl.Utf8,
        "left_external_id": pl.Utf8,
        "left_title": pl.Utf8,
        "left_location": pl.Utf8,
        "right_source": pl.Utf8,
        "right_external_id": pl.Utf8,
        "right_title": pl.Utf8,
        "right_location": pl.Utf8,
        "title_score": pl.Float64,
        "location_overlap": pl.Boolean,
        "status": pl.Utf8,
    }
    return pl.DataFrame(rows, schema=schema).sort(
        ["company_normalized", "title_score"], descending=[False, True]
    )
