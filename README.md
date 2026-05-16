# airflow-dlt

YAML-driven MSSQL → Postgres pipelines using [Apache Airflow](https://airflow.apache.org/)
and [dlt](https://dlthub.com/).

A single Airflow DAG (`dlt_pipeline`) reads a YAML file describing one
pipeline — source endpoint, target endpoint, table list, write semantics — and
runs the load via dlt. Credentials are resolved through a `SecretsClient`
interface; a `MockDelineaClient` (local YAML file) ships out of the box, and a
real Delinea integration can drop in behind the same interface without
touching any pipeline code.

## Quick start

```bash
# 1. Copy the secrets template and edit it
cp config/secrets.yaml.example config/secrets.yaml

# 2. Bring up Airflow + MSSQL + Postgres
docker compose up -d --build

# 3. Trigger the example pipeline from the Airflow UI (http://localhost:8080)
#    DAG: dlt_pipeline
#    Conf: {"config_name": "stackoverflow"}
```

## Repo layout

```
airflow-dlt/
├── dags/dlt_pipeline.py            # The (single) parameterized DAG
├── plugins/airflow_dlt/
│   ├── config.py                   # Pydantic models + YAML loader
│   ├── secrets.py                  # SecretsClient interface
│   ├── secrets_mock.py             # MockDelineaClient (YAML-backed)
│   ├── dlt_pipeline.py             # build_pipeline(cfg, secrets) → (pipeline, source)
│   └── schema_naming.py            # Target dataset name derivation
├── config/
│   ├── pipelines/stackoverflow.yaml   # Example pipeline definition
│   ├── secrets.yaml.example           # Template (committed)
│   └── secrets.yaml                   # Real secrets (gitignored)
├── tests/                          # Unit tests for plugins + DAG integrity
├── docker-compose.yml              # Airflow + MSSQL + Postgres
├── Dockerfile                      # Airflow image with ODBC 18 + dlt
└── pyproject.toml
```

## Pipeline YAML schema

```yaml
pipeline:
  name: stackoverflow_mssql_to_postgres   # also the dlt pipeline_name (load-bearing)
  schedule: null                          # cron string or null
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

Add more pipelines by dropping additional YAML files into
`config/pipelines/` and triggering the DAG with the matching `config_name`.

## Triggering

```json
{"config_name": "stackoverflow"}
```

The DAG looks up `config/pipelines/<config_name>.yaml`.

## Credentials

`config/secrets.yaml` (gitignored) maps `secret_id` → `{username, password}`.
The `MockDelineaClient` reads it at task runtime. Swap in a real Delinea
client by implementing `SecretsClient.get` and threading it through the DAG.

## Testing

```bash
# Plugin unit tests (no Airflow runtime required)
PYTHONPATH=plugins uv run --no-project \
  --with "dlt[sql_database]" --with pydantic --with pyyaml --with pytest \
  pytest tests/plugins -v

# DAG integrity tests (requires Airflow installed)
uv sync
uv run pytest tests/dags -v
```

## What dlt replaces

This repo is a DLT-first reimagining of the [`mssql-to-postgres-pipeline`](../mssql-to-postgres-pipeline)
template. The custom extract/load/state machinery in that repo
(`data_transfer.py`, `schema_extractor.py`, `ddl_generator.py`,
`type_mapping.py`, `binary_copy.py`, `incremental_state.py`) is all replaced
by dlt's `sql_database` source + `postgres` destination. What remains here is
roughly 200 lines of glue: config loading, secrets resolution, dataset
naming, and a 30-line DAG.
