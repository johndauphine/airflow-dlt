# Postgres To Postgres Benchmark

This benchmark verifies that Postgres sources can move to Postgres targets
through the same `dlt_pipeline` DAG shape used for SQL Server migrations.

It uses synthetic Stack Overflow-like tables generated directly inside a
Postgres container. The data is intentionally large enough to exercise
extraction, local dlt package staging, mapped Airflow tasks, and Postgres
destination loading.

## Environment

- Source container: `pg-bench`
- Source database: `so2013_dlt`
- Source schema: `so_pg_src`
- Target container: `postgres-target`
- Target database: `so_pg_verify`
- Target schema: `pgsynthetic_so2013_dlt_so_pg_src`
- DAG: `dlt_pipeline`
- Config: `config/pipelines/postgres_stackoverflow_synthetic_bench.yaml`
- dlt settings: `sql_backend: pyarrow`, `loader_file_format: parquet`,
  `load_workers: 5`

The Airflow worker must be able to reach both `pg-bench` and
`postgres-target`. In the local benchmark setup, `pg-bench` lives on
`dmt-bench`, so attach Airflow containers to that network:

```bash
for c in airflow-webserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer; do
  docker network connect dmt-bench "$c" 2>/dev/null || true
done
```

## Secrets

Add a source secret for `pg-bench` to local `config/secrets.yaml`:

```yaml
secrets:
  source_pg_bench:
    username: postgres
    password: TestPass2024

  target_postgres_main:
    username: postgres
    password: PostgresPassword123
```

`config/secrets.yaml` is gitignored. Do not commit real credentials.

## Target Database

Create the target database once:

```bash
docker exec postgres-target psql -U postgres -d postgres \
  -c "create database so_pg_verify;"
```

If rerunning from scratch:

```bash
docker exec postgres-target psql -U postgres -d postgres \
  -c "drop database if exists so_pg_verify;"
docker exec postgres-target psql -U postgres -d postgres \
  -c "create database so_pg_verify;"
```

## Source Data Shape

The synthetic source schema has nine tables:

| Table | Rows | Approx source size |
| --- | ---: | ---: |
| `posts` | `8,000,000` | `4899 MB` |
| `comments` | `12,000,000` | `3751 MB` |
| `votes` | `20,000,000` | `1002 MB` |
| `users` | `1,000,000` | `326 MB` |
| `badges` | `3,000,000` | `195 MB` |
| `post_links` | `1,000,000` | `58 MB` |
| `post_types` | `5` | `24 kB` |
| `vote_types` | `9` | `24 kB` |
| `link_types` | `2` | `24 kB` |

Total source relation size: `10231 MB`.

## Seed The Source Schema

The source data was generated with SQL in `pg-bench`. The generator uses
`generate_series`, deterministic md5 text payloads, and unlogged tables for
fast local setup.

Use the script in
[`postgres-stackoverflow-synthetic-seed.sql`](postgres-stackoverflow-synthetic-seed.sql):

```bash
docker exec -i pg-bench psql -U postgres -d so2013_dlt -v ON_ERROR_STOP=1 \
  < docs/benchmarks/postgres-stackoverflow-synthetic-seed.sql
```

Verify the source:

```bash
docker exec pg-bench psql -U postgres -d so2013_dlt -c "
select c.relname,
       pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
       c.reltuples::bigint as estimated_rows
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'so_pg_src'
  and c.relkind = 'r'
order by pg_total_relation_size(c.oid) desc;"
```

## Run The DAG

```bash
RUN_ID="manual__pg_synthetic_$(date -u +%Y%m%dT%H%M%SZ)"

docker exec airflow-worker airflow dags trigger dlt_pipeline \
  -r "$RUN_ID" \
  -c '{"config_name":"postgres_stackoverflow_synthetic_bench"}'
```

Monitor:

```bash
docker exec airflow-worker airflow tasks states-for-dag-run dlt_pipeline "$RUN_ID"

docker exec postgres-target psql -U postgres -d so_pg_verify -c "
select schemaname, relname, n_live_tup,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', schemaname, relname)::regclass)) as size
from pg_stat_user_tables
where schemaname like 'pgsynthetic%'
order by pg_total_relation_size(format('%I.%I', schemaname, relname)::regclass) desc;"

docker exec airflow-worker bash -lc 'du -sh /opt/airflow/.dlt'
docker stats --no-stream airflow-worker pg-bench postgres-target
```

## Measured Result

Measured on 2026-05-18:

- Run id: `manual__pg_synthetic_20260518T163553Z`
- DAG state: `success`
- DAG runtime: about `1m08s`
- Source rows: `45,000,016`
- Target rows: `45,000,016`
- Source size: `10231 MB`
- Target schema size: `11 GB`
- Final dlt working directory size: `12M`
- All nine mapped table tasks succeeded.

Exact row counts:

| Table | Source rows | Target rows |
| --- | ---: | ---: |
| `badges` | `3,000,000` | `3,000,000` |
| `comments` | `12,000,000` | `12,000,000` |
| `link_types` | `2` | `2` |
| `post_links` | `1,000,000` | `1,000,000` |
| `post_types` | `5` | `5` |
| `posts` | `8,000,000` | `8,000,000` |
| `users` | `1,000,000` | `1,000,000` |
| `vote_types` | `9` | `9` |
| `votes` | `20,000,000` | `20,000,000` |

Task timing from Airflow:

| Map index | Table | Runtime |
| ---: | --- | ---: |
| `0` | `badges` | `11s` |
| `1` | `comments` | `53s` |
| `2` | `link_types` | `4s` |
| `3` | `post_links` | `6s` |
| `4` | `post_types` | `3s` |
| `5` | `posts` | `60s` |
| `6` | `users` | `11s` |
| `7` | `vote_types` | `3s` |
| `8` | `votes` | `51s` |

The run demonstrates that the generic Postgres source connector, dynamic table
mapping, dlt parquet staging, and Postgres destination path work for a
multi-table 10 GB-class migration.
