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

### Identity rules

1. **Same source:** `(source, external_id)` is unique. If a run sees it twice, keep the latest.
2. **Across sources, confident:** records sharing an `ats_job_key` are the same opportunity
   (`match_method = ats_id`). Otherwise records sharing `apply_url_normalized` are the same
   (`match_method = url`).
3. **Across sources, uncertain:** same `company_normalized` and similar titles but no shared
   ID or URL go to `review_candidates.parquet`. They are **not** merged automatically.

`opportunity_key` is the `ats_job_key`, else the normalized URL, else `{source}:{external_id}`.
