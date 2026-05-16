# Notes for Claude

This repo is a DLT-first Airflow pipeline. The template it's derived from
(`../mssql-to-postgres-pipeline`) does the same job with ~3000 lines of hand-written
ETL; here, dlt handles the load and only the YAML/secrets/DAG glue is ours.

## Architecture

- **DAG factory**, `dags/dlt_pipeline.py`. At parse time it walks
  `config/pipelines/*.yaml` and registers one DAG per file. The DAG's
  `dag_id`, `schedule`, `retries`, `retry_delay`, and `max_active_runs` all
  come from the YAML. A YAML that fails to parse registers a *broken* DAG
  (named `dlt_broken__<filename>`) whose only task fails loudly with the
  parse error — silent missing DAGs are operationally invisible.
- **Pipeline config** is a Pydantic model in `plugins/airflow_dlt/config.py`.
  Extra keys are rejected (`extra="forbid"`). `source` and `target` are
  **discriminated unions on `type`**, so each endpoint type has its own
  schema (MSSQL needs `driver`+`options`, Postgres needs `sslmode`, etc.).
- **Connectors** in `plugins/airflow_dlt/connectors.py` own the per-type
  glue: `sqlalchemy_url(creds)` for sources, `build_destination(creds)` for
  targets. Adding a new endpoint type = new Cfg model in `config.py` + new
  connector class + register it in `SOURCE_CONNECTORS` / `TARGET_CONNECTORS`.
  Everything else (DAG factory, dataset naming, secrets) is type-agnostic.
- **Credentials** never appear in YAML. The YAML references a `secret_id`;
  `SecretsClient` (interface) + `MockDelineaClient` (YAML-backed impl)
  resolve them at runtime. To plug in real Delinea, implement
  `SecretsClient.get` and swap the client in the DAG.
- **Dataset naming**: dlt's `dataset_name` doubles as the Postgres schema.
  We derive `{alias}__{db}__{schema}` when the source has a schema, and
  `{alias}__{db}` when it doesn't (e.g. MySQL, SQLite). No hardcoded
  fallback names. Don't rename `pipeline.name` casually — dlt state is
  keyed on it, and so is the DAG ID.

## When editing

- Don't add ENV-based config fallbacks; the move away from `.env` is deliberate.
- Per-table `overrides` go through `_apply_table_overrides` — if you add a new
  override field in `TableOverride`, update that function too.
- URL/credential construction lives in `connectors.py` (unit-tested in
  `tests/plugins/test_connectors.py`). Use `urllib.parse.quote(safe="")`
  for URL userinfo — never `quote_plus` (SQLAlchemy URL parsing reads
  `+` as a literal, silently corrupting creds containing spaces).
- If you add a new YAML field to `PipelineMeta` that should affect Airflow,
  wire it through `_make_dag` in `dags/dlt_pipeline.py` — otherwise it's inert.

## Running tests

Three layers:

1. **Plugin unit tests** (no Airflow, no Docker):
   ```bash
   PYTHONPATH=plugins uv run --no-project \
     --with "dlt[sql_database]" --with pydantic --with pyyaml --with pytest \
     pytest tests/plugins -v
   ```
2. **Plugin + DAG integrity tests** (requires `uv sync`):
   ```bash
   uv run pytest tests/ -v
   ```
3. **Integration tests** — `tests/integration/`, marked `@pytest.mark.integration`,
   excluded by default via `addopts = "-m 'not integration'"` in pyproject.
   Requires Docker, unixODBC, and ODBC Driver 18 on the host. Spins up real
   MSSQL + Postgres via `testcontainers`, runs `build_pipeline(...).run()`,
   asserts seeded rows land in Postgres.
   ```bash
   uv sync --extra dev --extra integration
   uv run pytest tests/integration -m integration -v
   ```

When adding integration tests:
- Reuse the session-scoped `mssql_container` / `postgres_container` fixtures
  in `tests/integration/conftest.py` — they pay the ~30s SQL Server boot once.
- Don't import `testcontainers` at module top — guard with `pytest.importorskip`
  so hosts without it still collect the rest of the test suite cleanly.

## What's deliberately not here

- No custom incremental state table (`_migration._migration_state`). dlt
  manages state in the destination under `_dlt_*` tables.
- No COPY/binary-COPY logic. dlt picks fast paths internally.
- No notification module. Re-add via Airflow's native `on_failure_callback`
  if/when Airflow 3.x supports callbacks; the template hit that same wall.
- No multi-DAG schema/migration/incremental split. dlt handles all three
  modes (`replace` / `append` / `merge`) via `write_disposition`.
