# SQL Server To Postgres Benchmark

This page is the short benchmark summary for SQL Server sources migrating to
Postgres targets through `dlt_pipeline`.

Detailed run notes, A/B loader comparisons, and incremental-load lessons are
in [StackOverflow2013 dlt/Airflow benchmark notes](stackoverflow2013-dlt.md).

## Environment

- Source container: `mssql-bench`
- Source database: `StackOverflow2013`
- Source schema: `dbo`
- Target container: `pg-bench`
- Target database: `so2013_dlt`
- DAG: `dlt_pipeline`
- Config: `config/pipelines/stackoverflow2013_bench.yaml`
- dlt settings: `sql_backend: pyarrow`, `loader_file_format: parquet`,
  `load_workers: 5`

The benchmark uses the Brent Ozar StackOverflow2013 SQL Server database. The
database backup itself is not committed to this repo.

## Run

```bash
docker network create dmt-bench 2>/dev/null || true
docker volume create mssql-bench-data
docker volume create pg-bench-data
docker compose -f docker-compose.bench.yml up -d

for c in airflow-webserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer; do
  docker network connect dmt-bench "$c" 2>/dev/null || true
done

docker exec airflow-worker airflow dags trigger dlt_pipeline \
  -r dlt_so2013_full_parquet_adbc_$(date -u +%Y%m%dT%H%M%SZ) \
  -c '{"config_name":"stackoverflow2013_bench"}'
```

## Measured Result

Measured on 2026-05-18:

- Run id: `dlt_so2013_full_parquet_adbc_20260518T145040Z`
- DAG state: `success`
- DAG runtime: `3m52.77s`
- Long-pole table: `Posts`, `3m47.40s`
- Final dlt working directory size after cleanup: `11M`
- All nine mapped table tasks succeeded.

Target row counts:

| Table | Rows |
| --- | ---: |
| `Badges` | `8,042,013` |
| `Comments` | `24,534,730` |
| `LinkTypes` | `2` |
| `PostLinks` | `1,421,208` |
| `Posts` | `17,142,169` |
| `PostTypes` | `8` |
| `Users` | `2,464,749` |
| `Votes` | `52,928,720` |
| `VoteTypes` | `15` |

Total rows: `106,533,614`.

## Loader Lesson

The fastest path measured so far is:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: parquet
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Compared with CSV COPY in one-table A/B tests, Parquet/ADBC was about `40%`
faster for `Votes` and about `75%` faster for text-heavy `Posts`.
