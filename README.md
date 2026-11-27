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

### Docker (reproducible environment)

The same environment, pinned by `uv.lock`, also runs in Docker — no local
Python or uv needed. `.env` is optional; defaults work out of the box.

```bash
docker compose build
docker compose run --rm app uv run pytest -q              # test suite
docker compose run --rm app uv run python -m ingestion.run
docker compose run --rm app uv run python -m transform.run
docker compose up -d postgres                             # database only
```

The repo is bind-mounted into the container, so code edits apply without a
rebuild; rebuild only when dependencies change. Set `POSTGRES_PORT` in `.env`
if 5432 is taken on your machine.

## Running the pipeline (so far)

```bash
uv run python -m ingestion.run     # collect from all sources
uv run python -m transform.run     # clean + deduplicate the latest run
uv run pytest                      # tests use saved fixtures, no network
```

`ingestion.run` also accepts `--only greenhouse|lever|github`.

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

## Testing, CI, and dedup accuracy

Three layers, all offline (saved API responses, no network):

1. **Unit tests** (`tests/unit/`): adapters (including feed failures,
   incomplete collections, and retry behavior), normalization, content
   hashing, and duplicate detection.
2. **End-to-end tests** (`tests/test_pipeline_fixtures.py`): the full
   ingest → clean → dedup pipeline on saved source responses, including
   idempotent replay.
3. **Accuracy evaluation** (`evaluation/`): a manually labeled sample of
   posting pairs (`evaluation/labeled_pairs.csv`) is scored against the
   pipeline's matches, reporting precision/recall, *missed duplicates*
   (and whether the review queue caught them) and *incorrect matches*:

   ```bash
   uv run python -m evaluation.dedup_accuracy          # human-readable report
   uv run python -m evaluation.dedup_accuracy --json
   ```

   It exits non-zero on any incorrect match or on a missed duplicate the
   review queue did not catch, so regressions fail CI.

GitHub Actions runs on every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)): ruff lint, the full
pytest suite, the accuracy evaluation, and a Docker build check that re-runs
the tests inside the container. Postgres-backed storage tests have a separate
workflow.

## Project structure

```
ingestion/   source adapters (Greenhouse, Lever, GitHub list) + run script
transform/   Polars cleaning, normalization, deduplication
sql/         PostgreSQL schema and views
dags/        Airflow DAGs
app/         Streamlit dashboard
nlp/         sponsorship-statement classifier (IDS 703 extension)
config/      source and employer configuration
docs/        data contract, ERD, architecture diagram
tests/       unit + fixture-based end-to-end tests
evaluation/  labeled posting pairs + dedup accuracy evaluation
```

## Team roles

| Member | Area |
|---|---|
| Zilu Jin & Agastya Vinchhi | Repository setup, data ingestion, cleaning and transformation |
| Yiran Chen | Storage (PostgreSQL), ERD, SQL queries and views |
| Jacey Dai | Orchestration (Airflow) and reliability |
| Tsaone Tapologo | Containerization, CI/CD, and testing |

## Data engineering principles

*To be completed by the team. Examples already in the code:*

- **Modularity / reusability:** every adapter returns the same `RawRecord`
  shape (`ingestion/base.py`); adding a source means adding one file.
- **Reliability:** HTTP retries with backoff; each collection is checked for
  completeness (Greenhouse `meta.total`, Lever pagination) and flagged in the
  run manifest instead of failing silently.
- **Idempotency:** replaying the same batch produces identical output
  (`tests/test_pipeline_fixtures.py`).
- **Governance / lineage:** raw responses are stored unchanged with a SHA-256
  checksum, and every record carries its `run_id` and `snapshot_path`.
- **Efficiency:** `content_hash` lets later steps skip descriptions that
  haven't changed.
- **Security:** credentials live in `.env`, which is git-ignored.
- **Reproducibility / observability:** the Docker image installs exactly
  `uv.lock`; CI runs lint, tests, the dedup accuracy report, and an
  in-container test run on every push.
