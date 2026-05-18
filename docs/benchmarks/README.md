# Benchmarks

This directory records local benchmark results for large migrations through
the Airflow + dlt stack.

## Current Headline Results

| Scenario | Source | Target | Rows | Source size | Target size | Runtime | Notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| Full StackOverflow2013 | SQL Server `mssql-bench.StackOverflow2013.dbo` | Postgres `pg-bench.so2013_dlt` | `106,533,614` | Real SO2013 backup | Postgres schemas in `so2013_dlt` | `3m52.77s` | 9 mapped table tasks, Parquet/ADBC path |
| Synthetic Stack Overflow-like | Postgres `pg-bench.so2013_dlt.so_pg_src` | Postgres `postgres-target.so_pg_verify` | `45,000,016` | `10231 MB` | `11 GB` | `1m08s` | 9 mapped table tasks, Parquet/ADBC path |

Detailed notes:

- [SQL Server to Postgres](sql-server-to-postgres.md)
- [Detailed StackOverflow2013 SQL Server notes](stackoverflow2013-dlt.md)
- [Postgres to Postgres: synthetic Stack Overflow-like benchmark](postgres-to-postgres.md)

## Benchmark Principles

- Verify source and target row counts.
- Record DAG runtime and long-pole task runtime.
- Record dlt working directory size after cleanup.
- Watch container memory and block I/O during the run.
- Keep benchmark YAML files committed when they are generally useful.
- Keep actual data and local secrets out of git.

## Shared Benchmark Containers

`docker-compose.bench.yml` defines:

- `mssql-bench` for SQL Server benchmark data.
- `pg-bench` for Postgres benchmark data and benchmark targets.
- External network `dmt-bench`.

Create the shared network and volumes once:

```bash
docker network create dmt-bench 2>/dev/null || true
docker volume create mssql-bench-data
docker volume create pg-bench-data
docker compose -f docker-compose.bench.yml up -d
```

Attach the Airflow runtime containers when a benchmark YAML uses `mssql-bench`
or `pg-bench` hostnames:

```bash
for c in airflow-webserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer; do
  docker network connect dmt-bench "$c" 2>/dev/null || true
done
```

The local target `postgres-target` from `docker-compose.yml` is on the default
Airflow network. The Airflow worker can reach it by hostname.
