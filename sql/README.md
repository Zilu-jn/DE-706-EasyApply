# PostgreSQL storage and SQL

EasyApply stores source postings, matched opportunities, posting history, requirement
evidence, student decisions, and collection outcomes in PostgreSQL. This component
connects the existing ingestion and Polars transformation outputs to reusable SQL views.

Storage and querying: Yiran Chen.

## Setup

From the repository root:

```bash
uv sync --frozen
cp .env.example .env
```

Set `POSTGRES_PASSWORD` in `.env` to a local development password. Export the file's
variables before running Python; the application does not automatically read `.env`.

```bash
set -a
source .env
set +a
```

For a standalone development database:

```bash
docker run --name easyapply-postgres --env-file .env \
  -p 127.0.0.1:5432:5432 \
  -v easyapply_pgdata:/var/lib/postgresql/data \
  -d postgres:16
docker exec easyapply-postgres pg_isready -U easyapply -d easyapply
uv run python -m sql.run init
```

Wait for `pg_isready` to report that PostgreSQL is accepting connections. The named
volume retains database contents when the container stops. Restart an existing
container with `docker start easyapply-postgres`.

The connection reads `DATABASE_URL` when provided, otherwise the `POSTGRES_*`
variables in `.env.example`. Within the team's Compose network, set `POSTGRES_HOST`
to the PostgreSQL service name. Raw and processed files must be available through
the same shared data volume used by ingestion, transformation, and Airflow.

## Load a collection

Run ingestion and note its collection ID. Replace `YOUR_RUN_ID` below with that ID:

```bash
uv run python -m ingestion.run
uv run python -m transform.run --run-id YOUR_RUN_ID
uv run python -m sql.run load --run-id YOUR_RUN_ID
uv run python -m sql.run summary
```

The loader reads the run's `manifest.json`, `postings.parquet`, and
`review_candidates.parquet`. `--data-dir` selects a different data directory.
Replaying the same run returns `already_loaded`. Changing an already registered
batch's files requires a new run ID.

Initialize the schema once before loading. Applied SQL files are checksummed;
subsequent schema changes belong in a new numbered migration. Initialization
registers all configured feeds, including feeds that have not been collected yet.

## Queries and decisions

Run the example queries with a PostgreSQL client:

```bash
docker exec -i easyapply-postgres psql -U easyapply -d easyapply < sql/example_queries.sql
```

The views and their row definitions are documented in
[`docs/database.md`](../docs/database.md). For example, the dashboard can query
`easyapply.v_current_opportunities`, `easyapply.v_pending_reviews`,
`easyapply.v_current_requirement_evidence`, and `easyapply.v_student_follow_up`.

Record a student decision using an `opportunity_key` returned by the database:

```bash
uv run python -m sql.run decision --student demo-student \
  --opportunity-key YOUR_OPPORTUNITY_KEY --status saved
```

Use `--status reviewed` after reviewing an update. Candidate review uses the
posting IDs returned by `v_pending_reviews`:

```bash
uv run python -m sql.run review --left 1 --right 2 \
  --status distinct --reviewer team
```

Review outcomes are retained across loads. `confirmed_duplicate` records a human
judgment for evaluation; it does not automatically rewrite opportunity identities
or merge student decisions.

## Reproducible demo

The demo starts from the committed real API-response samples. It then introduces
one clearly labeled synthetic description update and a synthetic feed failure.
Use a separate database so these examples remain separate from collected results:

```bash
docker exec easyapply-postgres createdb -U easyapply easyapply_demo
POSTGRES_DB=easyapply_demo uv run python -m sql.demo
```

If `DATABASE_URL` is set, point it at `easyapply_demo` as well. The demo accepts only
database names ending in `_demo` or `_test` and writes files under `data/demo/`.

On a fresh demo database, the result includes two confirmed cross-source
opportunities, one posting change, one student follow-up, an incomplete Greenhouse
feed with its last successful results retained, and `already_loaded` on replay.
The uncertain review queue is also populated. These fixture counts describe the
small demonstration, not the complete live collection.

## PostgreSQL integration tests

Create a dedicated test database. Tests replace only the `easyapply` schema in
that database and require a database name ending in `_test`:

```bash
docker exec easyapply-postgres createdb -U easyapply easyapply_test
export TEST_DATABASE_URL='postgresql://easyapply:YOUR_URL_ENCODED_PASSWORD@localhost:5432/easyapply_test'
REQUIRE_POSTGRES_TESTS=1 uv run pytest -q
uv run ruff check sql tests/integration tests/unit/test_storage_validation.py
uv run ruff format --check sql tests/integration tests/unit/test_storage_validation.py
```

Without `TEST_DATABASE_URL`, PostgreSQL integration tests skip while unit tests
still run. The focused [PostgreSQL workflow](../.github/workflows/postgres.yml)
starts PostgreSQL 16, installs the frozen dependencies, runs formatting and lint
checks for this component, and requires the entire test suite to execute.

## Team integration

| Component | Integration |
|---|---|
| Ingestion and transformation | Existing output filenames and identity rules are retained. |
| Airflow | Call `sql.load.load_run(data_dir, run_id)` after successful transformation. Supply the same explicit run ID through the tasks. |
| Docker Compose | Provide PostgreSQL 16, connection variables, a persistent database volume, and the shared data directory. |
| CI | The focused workflow can be incorporated into the team's broader pipeline. Database tests require a dedicated `_test` database. |
| Streamlit | Read the documented views and call `save_decision` or `resolve_candidate` for user actions. |
| Later classifiers | Store version-linked evidence with an explicit method name and original supporting excerpt. |

`load_run` returns counts and a load status and raises on database or validation
failure so Airflow can retry. Incomplete source collections are recorded as data
quality outcomes; they preserve that feed's last successful publication and remain
visible in `v_source_health`. The DAG can use these results to raise a source-health
alert without removing previously collected opportunities.
