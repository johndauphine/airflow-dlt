# Design

This document describes how the repo is put together and where changes should
usually land.

## Control Flow

```text
Airflow trigger conf
  {"config_name": "<yaml stem>"}
        |
        v
dags/dlt_pipeline.py
  read_config()
        |
        v
config/pipelines/<config_name>.yaml
        |
        v
PipelineConfig validation
        |
        v
one table_spec per table
        |
        v
run_table.expand(table_spec=...)
        |
        v
build_pipeline(cfg, secrets, table_names=[...], pipeline_name=...)
        |
        v
dlt sql_database source -> dlt pipeline.run() -> target destination
```

## Airflow DAG Shape

`dags/dlt_pipeline.py` is the manual DAG. It has two major tasks:

- `read_config()` reads and validates one YAML file, then emits JSON-friendly
  table specs.
- `run_table.expand(...)` dynamically maps one task instance per table.

For a single-table YAML, the configured `pipeline.name` is used exactly. For a
multi-table YAML, each mapped task receives a derived dlt pipeline name:

```text
{pipeline.name}_{sanitized_table_name}
```

That lets each table have independent dlt state while preserving a single
Airflow DAG run for the whole YAML.

Concurrency is controlled by the Airflow pool `dlt_table_loads`. The default
local pool size is 4 slots, created by `airflow-init`.

## YAML Schema

The schema lives in `plugins/airflow_dlt/config.py`.

Important properties:

- Pydantic models use `extra="forbid"`.
- Sources and targets are discriminated unions on `type`.
- `PipelineMeta` only carries `name`.
- Scheduling, retry policy, and parallelism do not belong in pipeline YAML.
- `tables.include` is the source of mapped table tasks.
- `tables.overrides` applies optional per-table dlt hints.

Supported source types:

- `mssql`
- `postgres`
- `sqlite`

Supported target types:

- `postgres`
- `sqlite`

## Connectors

`plugins/airflow_dlt/connectors.py` adapts validated config into runtime
objects:

- Source connectors produce SQLAlchemy URLs for dlt `sql_database`.
- Target connectors produce dlt destinations.

Adding a new endpoint type should be a narrow change:

1. Add a config model in `config.py`.
2. Add a connector class in `connectors.py`.
3. Register it in `SOURCE_CONNECTORS` or `TARGET_CONNECTORS`.
4. Add connector/config tests.

The DAG and `build_pipeline()` should not need endpoint-specific branching.

## Secrets

YAML files reference `secret_id`; they do not hold credentials.

The local runtime uses:

- `plugins/airflow_dlt/secrets_client.py` for the interface.
- `plugins/airflow_dlt/secrets_mock.py` for the YAML-backed implementation.
- `config/secrets.yaml` as the ignored local secret file.

A production secret manager should implement the same `SecretsClient.get`
contract.

## Dataset Naming

`plugins/airflow_dlt/schema_naming.py` derives the target dataset name. For
Postgres targets, the dlt dataset becomes the Postgres schema.

The pattern is:

```text
{schema_alias}_{source_database}_{source_schema}
```

For schemaless sources such as SQLite:

```text
{schema_alias}_{source_database}
```

The separator is a single underscore. This matters because the dlt Postgres
destination normalizes consecutive underscores in schema names.

## dlt Runtime Settings

Pipeline YAML can include a `dlt:` block for runtime behavior:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: parquet
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Benchmark results have favored `pyarrow` extraction plus `parquet` loader
files for large SQL Server and Postgres migrations into Postgres.

Avoid `normalize_workers` inside the Celery worker process. Airflow/Celery task
workers are daemonic, and child multiprocessing workers fail in that context.

## Docker Runtime

`docker-compose.yml` defines the local Airflow stack. `docker-compose.bench.yml`
defines shared benchmark database containers on the external `dmt-bench`
network.

The Airflow dag-processor service runs under a watchdog wrapper. It starts
`airflow dag-processor`, waits through a startup grace period, and then checks
that the `dlt_pipeline` DAG is freshly parsed. If freshness fails, it exits so
Docker restarts the service.

The watchdog is intentionally local-operational glue. It is not part of the
YAML schema.

## Restartability

The preferred restartability model is:

- Airflow task state identifies which table task failed or succeeded.
- dlt package/state metadata identifies what was loaded.
- Docker restart policies recover local service failures.

For a failed table load, rerun the task or DAG before changing the pipeline
shape. For process crashes, check `.dlt` package state and Airflow task logs;
dlt should resume or clean up through its own state model.
