# StackOverflow2013 dlt/Airflow benchmark notes

These notes capture local benchmark findings for loading the Brent Ozar StackOverflow2013 SQL Server database into Postgres through the Airflow `dlt_pipeline` DAG.

## Environment

- Source: SQL Server container `mssql-bench`, database `StackOverflow2013`, schema `dbo`.
- Target: Postgres container `pg-bench`, database `so2013_dlt`.
- Airflow executor: CeleryExecutor with one worker container.
- dlt state directory: `/opt/airflow/.dlt`.
- Cleanup is enabled in the Airflow image:
  - `LOAD__DELETE_COMPLETED_JOBS=true`
  - `LOAD__TRUNCATE_STAGING_DATASET=true`
- The benchmark database containers live on the external Docker network
  `dmt-bench`. After rebuilding the default Airflow stack, attach the Airflow
  runtime containers to that network before running benchmark configs that use
  `mssql-bench` and `pg-bench` hostnames:

```bash
for c in airflow-webserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer; do
  docker network connect dmt-bench "$c" 2>/dev/null || true
done
```

Cleanup removes loaded job payloads after successful loads, but data is still staged locally during extract/normalize/load.

## Slow path observed

The original full-table config used dlt defaults:

- `sql_database` backend: `sqlalchemy`
- Postgres loader format: default `insert_values`

That path spent a long time extracting/staging and did not begin loading to Postgres quickly. It reached multi-GB `.dlt` staging while still reading the source.

## Faster path tested

The faster config uses:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: csv
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Why it helps:

- `pyarrow` avoids row-by-row Python object/dict extraction overhead.
- `csv` lets the Postgres destination use `COPY` instead of `INSERT VALUES`.
- File rotation keeps intermediate packages split into manageable chunks.
- `load_workers` lets dlt upload/load files concurrently. Keep it modest
  when Airflow is also running multiple table tasks in parallel.

Do not set `normalize_workers` when running inside the Celery worker process. Airflow/Celery task workers are daemonic, and dlt multiprocessing normalize workers fail with:

```text
daemonic processes are not allowed to have children
```

## Votes benchmark result

Config: `config/pipelines/stackoverflow2013_votes_fast_bench.yaml`

- Table: `dbo.Votes`
- Source rows: `52,928,720`
- Target rows: `52,928,720`
- Run id: `dlt_so2013_votes_fast2_20260518T002500Z`
- Total runtime: `4m03.58s`
- Extract: `2m07.99s`
- Normalize: `1m16.66s`
- Load: `38.62s`
- Target schema: `fast_stackoverflow2013_dbo`
- Cleanup reduced the pipeline working directory to about `1.1M` after completion.

This shows the optimized path is viable for large initial dlt loads, although dlt still stages data locally.

## Parquet/ADBC load benchmark

Issue #6 tracks a follow-up benchmark for Postgres Parquet loading through
ADBC. The repo now includes two one-table `Votes` configs with the same
Airflow/dlt shape and `load_workers: 5` so the load format is the main
variable:

- `config/pipelines/stackoverflow2013_votes_csv_copy_bench.yaml`
- `config/pipelines/stackoverflow2013_votes_parquet_adbc_bench.yaml`

The Parquet/ADBC config uses:

```yaml
dlt:
  sql_backend: pyarrow
  loader_file_format: parquet
  data_writer_file_max_items: 100000
  normalize_file_max_items: 100000
  load_workers: 5
```

Run the A/B test as separate DAG runs:

```bash
airflow dags trigger dlt_pipeline \
  -r dlt_so2013_votes_csv_copy_$(date -u +%Y%m%dT%H%M%SZ) \
  -c '{"config_name":"stackoverflow2013_votes_csv_copy_bench"}'

airflow dags trigger dlt_pipeline \
  -r dlt_so2013_votes_parquet_adbc_$(date -u +%Y%m%dT%H%M%SZ) \
  -c '{"config_name":"stackoverflow2013_votes_parquet_adbc_bench"}'
```

Compare total task duration, dlt extract/normalize/load timings, target row
count, final `.dlt` working directory size, and container memory pressure. If
Parquet/ADBC wins on `Votes` without type errors, repeat the test on `Posts`
before considering a full-table SO2013 run.

Measured result on 2026-05-18:

| Config | Run id | Target schema | Rows | DAG runtime | `run_table` runtime | dlt load step | Final `.dlt` size |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `stackoverflow2013_votes_csv_copy_bench` | `dlt_so2013_votes_csv_copy_20260518T034944Z` | `csvcopy_stackoverflow2013_dbo` | `52,928,720` | `2m56.96s` | `2m55.60s` | `18.31s` | `7.6M` |
| `stackoverflow2013_votes_parquet_adbc_bench` | `dlt_so2013_votes_parquet_adbc_20260518T035355Z` | `adbc_stackoverflow2013_dbo` | `52,928,720` | `1m46.53s` | `1m45.48s` | `15.66s` | `8.6M` |

On this one-table `Votes` run, Parquet/ADBC was about `40%` faster end to end
than the matched CSV COPY config. Both runs loaded the same row count and
cleaned up local dlt package data successfully. The load-step delta was modest;
most of the win came before or around the destination load boundary, so repeat
the A/B test on a text-heavy table such as `Posts` before assuming the same
speedup applies to the full StackOverflow2013 load.

## Full SO2013 run

Config: `config/pipelines/stackoverflow2013_bench.yaml`

The full SO2013 config now uses the fast settings above. A full run was started as:

```bash
airflow dags trigger dlt_pipeline \
  -r dlt_so2013_full_fast_20260518T002900Z \
  -c '{"config_name":"stackoverflow2013_bench"}'
```

At the time these notes were written, that run was still extracting the largest tables (`Comments`, `Posts`, `Votes`). The single-table `Votes` result suggests the optimized full load should be materially faster than the original defaults, but the all-table run is still gated by source extraction and local staging for the large text-heavy tables.

## Incremental-load lesson

Plain `merge` is not enough for cheap repeat runs. dlt will still re-extract/re-stage the source unless an incremental cursor is configured.

The `Users` incremental/upsert benchmark showed the desired behavior:

- First run loaded `2,465,713` rows.
- Second run completed in about `1.35s` with `0 load package(s)`.
- Cursor state recorded `LastAccessDate.last_value`.

Use `merge + primary_key + incremental` for dlt-owned incremental maintenance after a baseline load.
