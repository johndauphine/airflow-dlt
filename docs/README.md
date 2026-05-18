# Documentation

Start here when you are new to the repo, whether you are a human operator or
an AI agent picking up an existing workspace.

## First Reading Path

1. [Getting started](getting-started.md) - local setup, Docker services,
   secrets, first DAG run, useful validation commands.
2. [Philosophy](philosophy.md) - why this repo exists and what it deliberately
   avoids.
3. [Design](design.md) - how YAML, Airflow, dlt, secrets, dynamic task mapping,
   and Docker fit together.
4. [Technical specification](tech-spec.md) - behavioral contracts, runtime
   assumptions, extension points, and testing requirements.
5. [Benchmarks](benchmarks/README.md) - measured migration results and how to
   reproduce the local benchmark setups.

## High-Signal Files

- [README.md](../README.md) - project overview and quick start.
- [CLAUDE.md](../CLAUDE.md) - working notes for AI agents and maintainers.
- [dags/dlt_pipeline.py](../dags/dlt_pipeline.py) - the single parameterized
  manual DAG.
- [plugins/airflow_dlt/dlt_pipeline.py](../plugins/airflow_dlt/dlt_pipeline.py)
  - builds the dlt pipeline/source pair from validated YAML.
- [plugins/airflow_dlt/config.py](../plugins/airflow_dlt/config.py) - the YAML
  schema.
- [plugins/airflow_dlt/connectors.py](../plugins/airflow_dlt/connectors.py) -
  source and destination adapters.
- [Technical specification](tech-spec.md) - contract-level reference for
  maintainers and agents.

## Operational Baseline

The local stack is Docker Compose based:

- Airflow 3.0 with CeleryExecutor.
- Postgres metadata database.
- Redis broker.
- Airflow webserver, scheduler, dag-processor, triggerer, and worker.
- Optional local MSSQL and Postgres data containers for examples.
- Optional benchmark containers in `docker-compose.bench.yml`.

The central invariant is that `dlt_pipeline` is the manual DAG. Trigger it
with a JSON config name, for example:

```bash
docker exec airflow-worker airflow dags trigger dlt_pipeline \
  -c '{"config_name":"example_postgres_to_postgres"}'
```

Each YAML file in `config/pipelines/` defines what to move. Airflow decides
when and how much can run at once.
