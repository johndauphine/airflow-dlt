# Notes for Claude

This repo is a DLT-first Airflow pipeline. The template it's derived from
(`../mssql-to-postgres-pipeline`) does the same job with ~3000 lines of hand-written
ETL; here, dlt handles the load and only the YAML/secrets/DAG glue is ours.

## Architecture

- **Single parameterized DAG**, `dags/dlt_pipeline.py`. Always `dag_id=dlt_pipeline`.
  Trigger with `{"config_name": "<yaml stem>"}` to pick which YAML to run.
  Scheduling, retries, max_active_runs are properties of the DAG itself —
  not per-YAML. (We tried the DAG-factory pattern earlier in development;
  it created confusing per-YAML DAGs whose operational knobs lived in a
  different place from where you'd normally configure Airflow.)
- **Local dag-processor watchdog** in `docker-compose.yml` checks the Airflow
  metadata DB for fresh parses of `dlt_pipeline`. If this DAG ID changes,
  update the watchdog command and healthcheck together. The custom knobs are
  `DLT_DAG_PROCESSOR_MAX_PARSE_AGE_SECONDS`,
  `DLT_DAG_PROCESSOR_STARTUP_GRACE_SECONDS`, and
  `DLT_DAG_PROCESSOR_WATCHDOG_INTERVAL`.
- **Pipeline config** is a Pydantic model in `plugins/airflow_dlt/config.py`.
  Extra keys are rejected (`extra="forbid"`). `source` and `target` are
  **discriminated unions on `type`**, so each endpoint type has its own
  schema (MSSQL needs `driver`+`options`, Postgres needs `sslmode`, etc.).
  `PipelineMeta` only carries `name` — scheduling fields are deliberately
  not part of the YAML schema.
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
  We derive `{alias}_{db}_{schema}` when the source has a schema, and
  `{alias}_{db}` when it doesn't (e.g. MySQL, SQLite). Single underscore
  separator deliberately — dlt's postgres destination collapses consecutive
  underscores in schema names, so `__` would silently become `_` at the
  destination and `pipeline.dataset_name` would diverge from the real
  postgres schema. No hardcoded
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
- Don't add scheduling/retry fields to `PipelineMeta` — they're inert under
  the single-DAG model and we've removed them once already. Schedule and
  retries are properties of the DAG, not properties of the pipeline.

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
3. **SQLite end-to-end smoke test** — `tests/integration/test_sqlite_smoke.py`.
   No `@pytest.mark.integration` marker, so it runs by default. Uses dlt's
   `sqlalchemy` destination + Python stdlib `sqlite3`; no Docker, no host
   drivers. This is the CI regression guard for the connector pattern.
   *Note*: dlt's sqlalchemy/SQLite destination writes data into a sibling
   file named `{target_stem}__{dataset_name}.db`, not the target path
   itself — the target.db acts as a state anchor. The test helper finds
   the data file by globbing, so it tolerates dlt's naming choices.
4. **Container-backed integration tests** — `tests/integration/test_*.py`
   except the SQLite smoke test. Marked `@pytest.mark.integration`,
   excluded by default. Requires Docker, unixODBC, and ODBC Driver 18 on
   the host. Spins up real MSSQL + Postgres via `testcontainers`.
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
