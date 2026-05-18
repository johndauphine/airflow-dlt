# Getting Started

This guide gets a fresh human or AI agent from clone to a working local DAG
run. It assumes macOS or Linux with Docker available.

## Prerequisites

- Docker Desktop or Docker Engine with Compose.
- `uv` for Python dependency management.
- Enough local disk for staging and database volumes. The full benchmark data
  can consume tens of GB.
- For host-run MSSQL integration tests only: unixODBC and ODBC Driver 18.

The Docker image used by the Airflow stack already includes the runtime
dependencies needed for Airflow tasks, dlt, Postgres, ADBC, pyodbc, and SQL
Server connectivity.

## Clone And Configure

```bash
git clone <repo-url> airflow-dlt
cd airflow-dlt

cp config/secrets.yaml.example config/secrets.yaml
```

Edit `config/secrets.yaml` for the local services you plan to use. The file
is gitignored and must not be committed. For the default local Compose stack,
the target Postgres password is usually:

```yaml
secrets:
  target_postgres_main:
    username: postgres
    password: PostgresPassword123
```

The example SQL Server benchmark containers in `docker-compose.bench.yml` use
`TestPass2024` unless overridden with environment variables.

## Start The Local Airflow Stack

```bash
docker compose up -d --build
docker compose ps
```

Open Airflow at [http://localhost:8080](http://localhost:8080).

Default credentials:

- Username: `airflow`
- Password: `airflow`

The stack creates the `dlt_table_loads` pool during `airflow-init`. That pool
is the table-level concurrency control for mapped dlt table tasks.

## Run The Default Example

The default `stackoverflow.yaml` points at the local SQL Server and Postgres
containers from `docker-compose.yml`.

```bash
docker exec airflow-worker airflow dags trigger dlt_pipeline \
  -r local_stackoverflow_$(date -u +%Y%m%dT%H%M%SZ) \
  -c '{"config_name":"stackoverflow"}'
```

Check the run:

```bash
docker exec airflow-worker airflow dags list-runs dlt_pipeline
docker exec airflow-worker airflow tasks states-for-dag-run dlt_pipeline <run_id>
```

The seed SQL creates empty Stack Overflow-like SQL Server tables. To run a real
load, restore a real Stack Overflow backup or use one of the benchmark setups.

## Run The Postgres-To-Postgres Example

The committed example config is
`config/pipelines/example_postgres_to_postgres.yaml`. It expects a source
Postgres host named `source-pg`, so it is primarily a template. For a local
large benchmark that uses `pg-bench` as the source, see
[Postgres to Postgres benchmark](benchmarks/postgres-to-postgres.md).

## Useful Health Checks

```bash
docker compose ps
docker inspect --format '{{.State.Status}} {{.State.Health.Status}} {{.RestartCount}}' \
  airflow-worker airflow-scheduler airflow-dag-processor

docker exec airflow-dag-processor bash -lc \
  'PYTHONPATH=/opt/airflow/plugins python -m airflow_dlt.healthcheck \
   dag-processor-fresh --dag-id dlt_pipeline --max-age-seconds 300'
```

The dag-processor container has a watchdog wrapper. It checks that
`dlt_pipeline` is being freshly parsed and exits the container when the DAG
goes stale, allowing Docker to restart it.

Custom watchdog knobs:

- `DLT_DAG_PROCESSOR_MAX_PARSE_AGE_SECONDS`
- `DLT_DAG_PROCESSOR_STARTUP_GRACE_SECONDS`
- `DLT_DAG_PROCESSOR_WATCHDOG_INTERVAL`

## Running Tests

Fast local checks:

```bash
uv sync --extra dev
uv run pytest tests/plugins tests/dags
uv run ruff check dags plugins tests
```

Full default test suite:

```bash
uv run pytest tests
```

Container-backed integration tests:

```bash
uv sync --extra dev --extra integration
uv run pytest tests/integration -m integration -v
```

Integration tests spin up their own containers with testcontainers and are
excluded from default pytest runs.

## Notes For AI Agents

- Read `CLAUDE.md` before changing code. It captures repo-specific invariants
  that are not obvious from generic Airflow/dlt knowledge.
- Do not commit `config/secrets.yaml`, Docker volumes, `.dlt`, or logs.
- Prefer adding a new YAML under `config/pipelines/` over changing an existing
  benchmark config in place.
- The manual DAG ID is `dlt_pipeline`. Do not rename it casually; docs,
  healthchecks, and operational habits depend on it.
- Multi-table YAML files should stay single YAML files. Airflow dynamic task
  mapping creates one task per table and pool slots limit concurrency.
- If a run fails, try a restart before redesigning the pipeline. dlt state and
  Airflow task state are part of the intended restartability story.
