# EasyApply

*Because job hunting should not be a full-time job.*

EasyApply collects data-related job and internship postings from several public
sources, cleans them, and figures out which listings are actually the same
opportunity. The goal is a single, deduplicated view where students can tell a
genuinely new opening from one they've already seen.

IDS 706 Data Engineering Systems, final project (Fall 2026).

## Data sources

| Source | What we get | How |
|---|---|---|
| [Greenhouse Job Board API](https://developers.greenhouse.io/job-board.html) | Full postings with descriptions from 6 employer boards | `GET /v1/boards/{token}/jobs?content=true` |
| [Lever Postings API](https://github.com/lever/postings-api) | Full postings with descriptions from 4 companies | `GET /v0/postings/{company}?mode=json`, paginated |
| [SimplifyJobs internship list](https://github.com/SimplifyJobs/Summer2027-Internships) | Community-maintained listings (no descriptions) | `listings.json`, pinned to a commit SHA each run |

Employers are listed in [`config/sources.yaml`](config/sources.yaml). We picked
companies that use Greenhouse or Lever **and** show up in the SimplifyJobs list,
so we can measure real cross-source duplicates.

## Setup

Requires [uv](https://docs.astral.sh/uv/) (or any Python 3.11+ environment).

```bash
uv sync                 # creates .venv and installs dependencies
cp .env.example .env    # fill in values; never commit .env
```

## Running the pipeline (so far)

```bash
uv run python -m ingestion.run     # collect from all sources
uv run python -m transform.run     # clean + deduplicate the latest run
uv run pytest                      # tests use saved fixtures, no network
```

`ingestion.run` also accepts `--only greenhouse|lever|github`.

### PostgreSQL storage and queries

The PostgreSQL component is implemented under [`sql/`](sql/README.md). Follow its
setup instructions to start PostgreSQL, export the connection variables, and initialize:

```bash
uv run python -m sql.run init
uv run python -m sql.run load --run-id YOUR_RUN_ID
uv run python -m sql.run summary
```

Use the same run ID that ingestion and transformation produced. The database
preserves posting history, source completeness, requirement evidence, matching
reviews, and student decisions. Reusable views support current opportunities,
new discoveries, changes, follow-up, and feed health. The
[database design](docs/database.md) includes the ERD and data dictionary;
[example SQL queries](sql/example_queries.sql) demonstrate downstream use.

The storage guide also includes a saved-source demo and PostgreSQL integration
test commands. Ordinary `pytest` skips database integration tests unless
`TEST_DATABASE_URL` selects a dedicated test database. The focused
[PostgreSQL workflow](.github/workflows/postgres.yml) requires them to run.

Outputs, all under `data/` (git-ignored):

```
data/raw/{source}/{date}/{run_id}/*.json        raw API responses (immutable)
data/staging/{run_id}/raw_records.parquet       all sources in one schema
data/staging/{run_id}/manifest.json             per-source status, counts, checksums
data/processed/{run_id}/postings.parquet        cleaned + deduplicated postings
data/processed/{run_id}/review_candidates.parquet   uncertain matches for review
data/processed/{run_id}/summary.json            run statistics
```

The column definitions and identity rules are in
[`docs/data_contract.md`](docs/data_contract.md). That file is the interface
the database, Airflow, and dashboard work builds on.

## How deduplication works

1. **Same source:** `(source, external_id)` is unique.
2. **Across sources, confident match:** Greenhouse job IDs and Lever posting IDs
   are globally unique. We parse them out of every URL, including GitHub list
   links, so the same posting is linked even when the titles differ
   (e.g. *"Bayesian Methods Co-op"* on GitHub vs. *"Co-Op, Bayesian Methods for
   Complex Decision Making"* on Greenhouse). Normalized URLs are the fallback.
3. **Uncertain:** same company and a near-identical title but different IDs go
   to `review_candidates.parquet` instead of being merged. For example, Toyota
   Research Institute has posted the same intern title under several Lever IDs,
   which could be separate openings or reposts.

First live run (2026-11-19): 4,055 records → 3,990 unique opportunities, 65
confirmed cross-source matches, 15 pairs sent to review.

## Project structure

```
ingestion/   source adapters (Greenhouse, Lever, GitHub list) + run script
transform/   Polars cleaning, normalization, deduplication
sql/         PostgreSQL schema, views, batch loader, decisions, and saved-source demo
dags/        Airflow DAGs
app/         Streamlit dashboard
nlp/         sponsorship-statement classifier (IDS 703 extension)
config/      source and employer configuration
docs/        data contract, ERD, architecture diagram
tests/       unit + fixture-based end-to-end tests
```

## Team roles

| Member | Area |
|---|---|
| Zilu Jin & Agastya Vinchhi | Repository setup, data ingestion, cleaning and transformation |
| Yiran Chen | Storage (PostgreSQL), ERD, SQL queries and views |
| Jacey Dai | Orchestration (Airflow) and reliability |
| Tsaone Tapologo | Containerization, CI/CD, and testing |

## Data engineering principles

Implemented examples, with database details in [`docs/database.md`](docs/database.md):

- **Modularity / reusability:** every adapter returns the same `RawRecord`
  shape (`ingestion/base.py`); adding a source means adding one file.
- **Reliability:** HTTP retries with backoff; each collection is checked for
  completeness (Greenhouse `meta.total`, Lever pagination) and flagged in the
  run manifest instead of failing silently.
- **Idempotency:** replaying the same batch produces identical output
  (`tests/test_pipeline_fixtures.py`). Database fingerprints and unique keys prevent
  duplicate rows on replay, and changed registered inputs are rejected.
- **Governance / lineage:** raw responses are stored unchanged with a SHA-256
  checksum, and every record carries its `run_id` and `snapshot_path`. Database
  observations connect source snapshots, collection outcomes, versions, and opportunities.
- **Efficiency:** `content_hash` lets later steps skip descriptions that
  haven't changed. Storage reuses unchanged versions and skips repeated evidence extraction.
- **Security:** credentials live in `.env`, which is git-ignored.
  Database connections read exported environment variables, and SQL binds input parameters.
- **Observability:** collection and load records expose errors, completeness,
  counts, and last successful publication through `v_source_health`.
- **Storage reliability:** failed or incomplete feeds preserve their last
  successfully published results; database transactions prevent partial publication.

