# Technical Specification

This specification defines the expected behavior and extension contracts for
`airflow-dlt`. It is intended for maintainers, operators, and AI agents making
changes to the repo.

## Purpose

`airflow-dlt` provides YAML-defined database migration pipelines orchestrated
by Apache Airflow and executed by dlt. The primary production-shaped use cases
are:

- SQL Server to Postgres migrations.
- Postgres to Postgres migrations.
- SQLite-backed local smoke tests.

The system should keep migration intent declarative while leaving scheduling,
concurrency, retry behavior, and operational visibility in Airflow.

## Functional Requirements

- A single manual DAG, `dlt_pipeline`, must be able to run any YAML file under
  `config/pipelines/` by receiving `{"config_name":"<yaml stem>"}`.
- A YAML file with multiple tables must produce one Airflow mapped task per
  table in a single DAG run.
- Airflow pool `dlt_table_loads` must govern table-level concurrency.
- A one-table YAML must preserve the configured `pipeline.name` exactly.
- A multi-table YAML must use per-table dlt pipeline names derived from
  `{pipeline.name}_{sanitized_table_name}`.
- Source and target credentials must be resolved by `secret_id`; credentials
  must not appear in pipeline YAML.
- dlt must own extraction, normalization, load package state, target loading,
  and incremental state.
- The local Docker stack must expose enough health checks to detect stale DAG
  parsing and failed runtime services.

## Non-Goals

- This repo does not implement a custom migration-state database.
- This repo does not generate vendor-specific DDL by hand.
- This repo does not own a custom binary COPY implementation.
- Pipeline YAML does not define Airflow schedules, retries, pools, or
  parallelism.
- The benchmark datasets and restored database backups are not committed to
  git.

## Runtime Architecture

```text
User/API
  triggers dlt_pipeline with {"config_name": "..."}
        |
        v
Airflow read_config task
  validates config/pipelines/<config_name>.yaml
  emits table specs
        |
        v
Airflow mapped run_table tasks
  one mapped task per source table
  pool = dlt_table_loads
        |
        v
airflow_dlt.dlt_pipeline.build_pipeline()
  resolves secrets
  builds dlt sql_database source
  builds dlt destination
        |
        v
dlt pipeline.run()
  extracts -> stages packages -> normalizes -> loads target
```

## Repository Components

| Component | Responsibility |
| --- | --- |
| `dags/dlt_pipeline.py` | Manual Airflow DAG, config loading task, dynamic task mapping, table-level execution. |
| `dags/example_scheduled.py` | Template for scheduled DAGs that point at a fixed YAML. |
| `plugins/airflow_dlt/config.py` | Pydantic schema for pipeline YAML. |
| `plugins/airflow_dlt/connectors.py` | Source SQLAlchemy URLs and dlt destination construction. |
| `plugins/airflow_dlt/dlt_pipeline.py` | Builds `(pipeline, source)` from config and secrets. |
| `plugins/airflow_dlt/schema_naming.py` | Target dataset/schema name derivation. |
| `plugins/airflow_dlt/secrets_client.py` | Secret lookup interface. |
| `plugins/airflow_dlt/secrets_mock.py` | Local YAML-backed secret lookup. |
| `plugins/airflow_dlt/healthcheck.py` | Local Airflow metadata freshness checks. |
| `config/pipelines/*.yaml` | Declarative migration definitions. |
| `docker-compose.yml` | Local Airflow and example database stack. |
| `docker-compose.bench.yml` | Shared benchmark SQL Server/Postgres containers. |

## Pipeline YAML Contract

Top-level keys:

- `pipeline`
- `source`
- `target`
- `tables`
- `load`
- `dlt`

`pipeline`:

```yaml
pipeline:
  name: stackoverflow2013_mssql_to_postgres_bench
```

`pipeline.name` must be alphanumeric/underscore. It is a dlt state key and
should be treated as stable.

`source` is a discriminated union on `type`.

Supported source types:

| Type | Required fields | Optional fields |
| --- | --- | --- |
| `mssql` | `secret_id`, `host`, `database`, `schema` | `port`, `driver`, `options` |
| `postgres` | `secret_id`, `host`, `database`, `schema` | `port`, `sslmode`, `options` |
| `sqlite` | `path` | `secret_id` |

`target` is a discriminated union on `type`.

Supported target types:

| Type | Required fields | Optional fields |
| --- | --- | --- |
| `postgres` | `secret_id`, `host`, `database`, `schema_alias` | `port`, `sslmode` |
| `sqlite` | `path`, `schema_alias` | `secret_id` |

`tables`:

```yaml
tables:
  include:
    - Posts
    - Votes
  overrides:
    Users:
      write_disposition: merge
      primary_key: Id
      incremental:
        cursor_path: LastAccessDate
```

`tables.include` must be non-empty. It is the source of dynamic task mapping.

`load`:

```yaml
load:
  write_disposition: replace
  chunk_size: 100000
```

`dlt`:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: parquet
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Supported `sql_backend` values:

- `sqlalchemy`
- `pyarrow`
- `pandas`
- `connectorx`

Supported `loader_file_format` values are the dlt-supported literals exposed
by `DltRuntimeConfig`, including `parquet`, `csv`, `jsonl`, and
`insert_values`.

## Airflow DAG Contract

`dlt_pipeline` must expose `config_name` as a DAG param.

`read_config()` must:

- Resolve `config_name` to `config/pipelines/<config_name>.yaml`.
- Reject traversal attempts or missing config files.
- Validate YAML through `PipelineConfig`.
- Create one table spec per table in `tables.include`.
- Detect duplicate sanitized table suffixes for multi-table configs.
- Return JSON-serializable table specs containing `config_name`, `table_name`,
  and `pipeline_name`.

`run_table()` must:

- Reload the YAML by `config_name`.
- Apply dlt runtime environment settings.
- Build a one-table dlt pipeline using `build_pipeline()`.
- Run `pipeline.run(source, write_disposition=..., chunk_size=...)`.
- Return summary metadata including table name, dataset, actual pipeline name,
  configured base pipeline name, and dlt load ids.
- Use Airflow pool `dlt_table_loads`.

## Dataset Naming Contract

For targets where dlt dataset names become schemas, dataset names are derived
as:

```text
{schema_alias}_{source_database}_{source_schema}
```

For schemaless sources:

```text
{schema_alias}_{source_database}
```

Names are lowercased and sanitized by `schema_naming.py`. Do not switch to a
double-underscore separator; the dlt Postgres destination normalizes repeated
underscores, which can make dlt state disagree with the real target schema.

## Secrets Contract

Pipeline YAML references `secret_id`. Runtime code asks a `SecretsClient` for
credentials:

```python
{"username": "...", "password": "..."}
```

The local implementation reads `config/secrets.yaml`. Production integrations
must implement the same `SecretsClient.get(secret_id)` behavior.

`config/secrets.yaml` is ignored by git. `config/secrets.yaml.example` may
contain local example defaults, but no real credentials.

## Docker And Operational Contract

`docker-compose.yml` runs the local stack:

- `postgres-metadata`
- `redis`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`
- `airflow-worker`
- local example SQL Server/Postgres services

`airflow-init` must create or update the `dlt_table_loads` pool:

```bash
airflow pools set dlt_table_loads "${DLT_TABLE_LOAD_POOL_SLOTS:-4}" \
  "Concurrent dlt table loads"
```

The dag-processor service must run a watchdog that:

- Starts `airflow dag-processor`.
- Forwards `TERM` and `INT` to the child process.
- Waits through `DLT_DAG_PROCESSOR_STARTUP_GRACE_SECONDS`.
- Checks freshness for DAG `dlt_pipeline`.
- Exits non-zero when the DAG is missing, stale, or too old after startup
  grace, allowing Docker restart policy to recover it.

## Performance Contract

For large SQL-to-Postgres migrations, the preferred baseline is:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: parquet
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Airflow controls table-level concurrency through `dlt_table_loads`; dlt
controls file-level load concurrency inside each mapped task through
`load_workers`.

Avoid `normalize_workers` in Airflow Celery tasks because daemonic worker
processes cannot spawn child processes.

## Observability

Operators should be able to answer:

- Is the DAG fresh? Use `airflow_dlt.healthcheck`.
- Is the DAG run queued, running, successful, or failed? Use
  `airflow dags list-runs`.
- Which mapped table task failed? Use `airflow tasks states-for-dag-run`.
- How much local staging remains? Check `/opt/airflow/.dlt`.
- Did row counts match? Query source and target tables directly.
- Did dlt finish all loads? Inspect target `_dlt_loads` tables.

## Testing Contract

Required layers:

- Plugin unit tests for config, connectors, secrets, schema naming, and
  healthcheck behavior.
- DAG integrity tests that parse `dlt_pipeline` and validate mapped task
  structure/pool assignment.
- SQLite end-to-end smoke test for cheap local dlt execution.
- Optional Docker-backed integration tests for live SQL Server/Postgres and
  Postgres/Postgres flows.

Use:

```bash
uv run pytest tests/plugins tests/dags
uv run ruff check dags plugins tests
```

For integration:

```bash
uv sync --extra dev --extra integration
uv run pytest tests/integration -m integration -v
```

## Extension Points

To add a source:

1. Add a source config model in `config.py`.
2. Add a `SourceConnector` implementation in `connectors.py`.
3. Register it in `SOURCE_CONNECTORS`.
4. Add config and connector tests.
5. Add an example YAML if useful.

To add a target:

1. Add a target config model in `config.py`.
2. Add a `TargetConnector` implementation in `connectors.py`.
3. Register it in `TARGET_CONNECTORS`.
4. Add config and connector tests.
5. Document any dataset/schema naming implications.

To add a benchmark:

1. Add or identify benchmark data setup.
2. Add a YAML config under `config/pipelines/`.
3. Record source/target sizes, row counts, run id, DAG runtime, task runtimes,
   dlt working directory cleanup, and container resource pressure.
4. Add a page under `docs/benchmarks/`.

## Compatibility Assumptions

- Airflow 3.0 dynamic task mapping is available.
- The local executor is CeleryExecutor.
- The local worker image includes dlt, pyarrow, ADBC Postgres, psycopg2, pyodbc,
  unixODBC, and ODBC Driver 18.
- Postgres target schemas are created by dlt.
- SQL Server connectivity uses ODBC Driver 18.
- Benchmark commands assume Docker container DNS names from the Compose files.

## Known Tradeoffs

- dlt stages package files locally before loading. Large migrations need enough
  disk for transient `.dlt` state even when cleanup is enabled.
- The manual DAG is intentionally generic, so scheduled production jobs should
  use small DAG files that point at fixed YAML configs.
- Each mapped table task has separate dlt state in multi-table YAMLs. This is
  deliberate for restartability and parallelism.
- The local watchdog queries the Airflow metadata database directly. It is an
  operational healthcheck for the Compose stack, not a portable Airflow plugin.
