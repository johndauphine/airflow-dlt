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
  load_workers: 20
```

Why it helps:

- `pyarrow` avoids row-by-row Python object/dict extraction overhead.
- `csv` lets the Postgres destination use `COPY` instead of `INSERT VALUES`.
- File rotation keeps intermediate packages split into manageable chunks.
- `load_workers` lets dlt upload/load files concurrently.

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
