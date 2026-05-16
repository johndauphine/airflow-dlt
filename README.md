# airflow-dlt

YAML-driven MSSQL → Postgres pipelines using [Apache Airflow](https://airflow.apache.org/)
and [dlt](https://dlthub.com/).

Drop a YAML file into `config/pipelines/`, and an Airflow DAG appears for it
on the next reparse. Each YAML defines one pipeline — source endpoint, target
endpoint, table list, write semantics — and the DAG runs the load via dlt.
Credentials are resolved through a `SecretsClient` interface; a
`MockDelineaClient` (local YAML file) ships out of the box, and a real
Delinea integration can drop in behind the same interface without touching
any pipeline code.

## Quick start

```bash
# 1. Copy the secrets template and edit it
cp config/secrets.yaml.example config/secrets.yaml

# 2. Bring up Airflow + MSSQL + Postgres
#    (mssql-init seeds an empty StackOverflow2010 DB matching the example YAML)
docker compose up -d --build

# 3. Open Airflow at http://localhost:8080 (airflow / airflow)
#    Trigger the dag: dlt_stackoverflow_mssql_to_postgres
```

The example pipeline loads zero rows out of the box (the seed creates empty
tables). Replace `mssql-init/seed.sql` (or restore the real StackOverflow
backup separately) to exercise the pipeline with real data.

## Repo layout

```
airflow-dlt/
├── dags/dlt_pipeline.py            # DAG factory — one DAG per YAML
├── plugins/airflow_dlt/
│   ├── config.py                   # Pydantic models + YAML loader
│   ├── connectors.py               # Source/Target connectors + registries
│   ├── secrets_client.py           # SecretsClient interface (NOT secrets.py — shadows stdlib)
│   ├── secrets_mock.py             # MockDelineaClient (YAML-backed)
│   ├── dlt_pipeline.py             # build_pipeline(cfg, secrets) → (pipeline, source)
│   └── schema_naming.py            # Target dataset name derivation
├── config/
│   ├── pipelines/stackoverflow.yaml   # Example pipeline definition
│   ├── secrets.yaml.example           # Template (committed)
│   └── secrets.yaml                   # Real secrets (gitignored)
├── mssql-init/seed.sql             # One-shot seed for the example
├── tests/                          # Unit tests for plugins + DAG integrity
├── docker-compose.yml              # Airflow + MSSQL + mssql-init + Postgres
├── Dockerfile                      # Airflow image with ODBC 18 + dlt
└── pyproject.toml
```

## Pipeline YAML schema

```yaml
pipeline:
  name: stackoverflow_mssql_to_postgres   # also the dlt pipeline_name & dag_id suffix
  schedule: null                          # cron string or null
  max_active_runs: 1
  retries: 3
  retry_delay_seconds: 30

source:
  type: mssql
  secret_id: source_mssql_stackoverflow   # key in secrets.yaml
  host: mssql-server
  port: 1433
  database: StackOverflow2010
  schema: dbo
  driver: "ODBC Driver 18 for SQL Server"
  options:
    TrustServerCertificate: "yes"

target:
  type: postgres
  secret_id: target_postgres_main
  host: postgres-target
  port: 5432
  database: stackoverflow
  schema_alias: dev                       # → dataset "dev__stackoverflow2010__dbo"

tables:
  include: [Users, Posts, Comments]
  overrides:                              # optional per-table hints
    Users:
      write_disposition: merge
      primary_key: Id
      incremental:
        cursor_path: LastAccessDate

load:
  write_disposition: replace              # default for tables w/o override
  chunk_size: 100000
```

Adding another pipeline is just another file under `config/pipelines/`. Its
DAG ID will be `dlt_<pipeline.name>`.

### Supported endpoint types

| Side    | Type        | Notes                                              |
|---------|-------------|----------------------------------------------------|
| Source  | `mssql`     | via pyodbc + ODBC Driver 18; `driver` + `options`  |
| Source  | `postgres`  | via psycopg2; optional `sslmode`                   |
| Target  | `postgres`  | via dlt's postgres destination                     |

Source/target configs are Pydantic discriminated unions on `type`. Add a new
endpoint type by writing one Cfg model in
[`plugins/airflow_dlt/config.py`](plugins/airflow_dlt/config.py) and one
connector in [`plugins/airflow_dlt/connectors.py`](plugins/airflow_dlt/connectors.py).
The rest of the pipeline (DAG factory, secrets, dataset naming) is
type-agnostic.

## Credentials

`config/secrets.yaml` (gitignored) maps `secret_id` → `{username, password}`.
The `MockDelineaClient` reads it at task runtime. Swap in a real Delinea
client by implementing `SecretsClient.get` and threading it through the DAG.

## Testing

Three layers, each more expensive than the last:

```bash
# 1. Plugin unit tests — no Airflow, no Docker required
PYTHONPATH=plugins uv run --no-project \
  --with "dlt[sql_database]" --with pydantic --with pyyaml --with pytest \
  pytest tests/plugins -v

# 2. Plugin + DAG integrity tests (default for the project venv)
uv sync --extra dev
uv run pytest tests/ -v

# 3. Integration tests — Docker required, ODBC 18 + unixODBC required on host
#    Spins up real MSSQL + Postgres via testcontainers, runs build_pipeline().run(),
#    asserts seeded rows land in Postgres.
brew install unixodbc msodbcsql18      # one-time on macOS; Linux: apt msodbcsql18 unixodbc-dev
uv sync --extra dev --extra integration
uv run pytest tests/integration -v -m integration
```

Integration tests are excluded from default runs via a pytest marker, so layer
1 and 2 stay fast and Docker-free.

## What dlt replaces

This repo is a DLT-first reimagining of the [`mssql-to-postgres-pipeline`](../mssql-to-postgres-pipeline)
template. The custom extract/load/state machinery in that repo
(`data_transfer.py`, `schema_extractor.py`, `ddl_generator.py`,
`type_mapping.py`, `binary_copy.py`, `incremental_state.py`) is all replaced
by dlt's `sql_database` source + `postgres` destination. What remains here is
~250 lines of glue: config loading, secrets resolution, dataset naming, and a
small DAG factory.
