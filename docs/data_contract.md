# Data Contract

This is the interface between ingestion/cleaning and the rest of the pipeline
(database, Airflow, tests, NLP). Columns are only ever **added**, never renamed
or removed, without telling the team.

## Stage 1: raw records (`ingestion/`)

Every adapter returns the same record shape. Output file:
`data/staging/{run_id}/raw_records.parquet`

| Column | Type | Notes |
|---|---|---|
| `source` | str | `greenhouse`, `lever`, or `github` |
| `external_id` | str | The posting's ID inside its source |
| `company_token` | str | Board/company token (`cloudflare`, `zoox`); null for GitHub |
| `company` | str | Company name as given by the source or our config |
| `title` | str | Job title as given |
| `location` | str | Location as given; multiple locations joined with ` / ` |
| `apply_url` | str | Application or posting URL as given |
| `description_html` | str | Job description (HTML for Greenhouse/Lever); null for GitHub |
| `posted_at` | datetime (UTC) | When the source says it was posted, if available |
| `updated_at` | datetime (UTC) | When the source says it was last updated, if available |
| `fetched_at` | datetime (UTC) | When we collected it |
| `run_id` | str | Collection run that produced this record |
| `snapshot_path` | str | Raw response file this record came from (lineage) |
| `extra` | str (JSON) | Source-specific fields, e.g. GitHub `sponsorship`, `terms`, `degrees` |

Each run also writes `data/staging/{run_id}/manifest.json` with one entry per
source/company: HTTP status, expected vs. received count, `is_complete`, checksum,
and error message. **An incomplete collection must never be used to mark
postings as closed.**

## Stage 2: cleaned postings (`transform/`)

Output file: `data/processed/{run_id}/postings.parquet`. All Stage 1 columns
are kept, plus:

| Column | Type | Notes |
|---|---|---|
| `company_normalized` | str | Lowercase, punctuation and legal suffixes removed |
| `title_normalized` | str | Lowercase, whitespace collapsed |
| `location_normalized` | str | Lowercase, common abbreviations expanded |
| `apply_url_normalized` | str | Canonical host, tracking params and trailing slash removed |
| `ats_job_key` | str | `greenhouse:{job_id}` or `lever:{posting_id}` parsed from the URL; null if unknown |
| `description_text` | str | Plain text from `description_html` |
| `content_hash` | str | SHA-256 of the normalized content; changes only when content changes |
| `is_target_role` | bool | Title matches our data-role keywords |
| `opportunity_key` | str | Cross-source identity key (see below) |
| `match_method` | str | `ats_id`, `url`, or `none` |
| `opportunity_id` | str | First 16 hex characters of SHA-1 of `opportunity_key`; emitted by the transformer |
| `group_size` | integer | Posting records sharing an opportunity in this transformed batch |
| `n_sources` | integer | Distinct sources sharing an opportunity in this transformed batch |

### Identity rules

1. **Same source:** `(source, external_id)` is unique. If a run sees it twice, keep the latest.
2. **Across sources, confident:** records sharing an `ats_job_key` are the same opportunity
   (`match_method = ats_id`). Otherwise records sharing `apply_url_normalized` are the same
   (`match_method = url`).
3. **Across sources, uncertain:** same `company_normalized` and similar titles but no shared
   ID or URL go to `review_candidates.parquet`. They are **not** merged automatically.

`opportunity_key` is the `ats_job_key`, else the normalized URL, else `{source}:{external_id}`.

## Stage 3: PostgreSQL (`sql/`)

The database loader consumes the selected run's Stage 1 `manifest.json` and Stage 2
`postings.parquet` and `review_candidates.parquet`. The upstream column names and
identity assignments remain unchanged. Every posting must have a matching manifest
collection, collection timestamp, and snapshot path.

`source_feeds` identifies Greenhouse/Lever feeds by `(source, company_token)` and
the GitHub feed by its configured repository. GitHub records retain that repository
and the collected commit in `extra`. Review pairs use the emitted `left_source`,
`left_external_id`, `right_source`, `right_external_id`, `title_score`, and
`location_overlap` fields. Database review outcomes are preserved across reloads.

The database separates source postings from shared opportunities, and distinct
content versions from run observations. Its `state_hash` supplements `content_hash`
with meaningful searchable/source metadata. Repository commit changes remain
observation lineage and do not create new posting versions. Batch `group_size` and
`n_sources` are derived again for current SQL views because different feeds can
have different last-successful collection times.

Only complete collections in successfully loaded runs publish current results.
Partial records and failures remain auditable without replacing last-successful
results. An absent community listing is not treated as proof of employer closure.
The schema, ERD, view definitions, and integration behavior are documented in
[`database.md`](database.md) and [`sql/README.md`](../sql/README.md).

