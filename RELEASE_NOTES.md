# Release notes

This file tracks user-visible changes to `airflow-dlt`, organized by the PR
that introduced them. No formal version tags yet — the project is still
in initial development, and `main` is the truth.

---

## Current state (`main` @ 5d45b7a) — May 16, 2026

### What it does

A YAML-driven Airflow + [dlt](https://dlthub.com/) pipeline platform. Drop a
YAML file describing a source endpoint, target endpoint, and table list into
`config/pipelines/`. Trigger the single `dlt_pipeline` DAG with
`{"config_name": "<yaml stem>"}` and the data moves. Credentials are
resolved through a `SecretsClient` interface; a mock (YAML-backed)
implementation ships, with a clear seam for a real Delinea integration.

### Supported endpoint types

| Side | Type | Notes |
|---|---|---|
| Source | `mssql` | pyodbc + ODBC Driver 18 |
| Source | `postgres` | psycopg2; optional `sslmode` |
| Source | `sqlite` | file path; no auth — ideal for CI |
| Target | `postgres` | dlt postgres destination |
| Target | `sqlite` | dlt sqlalchemy destination — also for CI without a DB server |

Adding a new endpoint type = one `Cfg` model in
[`plugins/airflow_dlt/config.py`](plugins/airflow_dlt/config.py) + one
connector class in
[`plugins/airflow_dlt/connectors.py`](plugins/airflow_dlt/connectors.py) +
one registry entry. The rest of the pipeline (DAG factory, dataset naming,
secrets resolution, table overrides) is type-agnostic.

### Tests — 51 passing, ruff clean

| Layer | Count | Needs |
|---|---|---|
| Plugin unit (config, connectors, secrets, naming) | ~40 | nothing |
| DAG integrity (parses, has tags, resolver) | ~8 | airflow installed |
| SQLite end-to-end smoke (real `pipeline.run()`) | 2 | dlt + sqlite stdlib only — runs in default CI |
| MSSQL → Postgres integration (testcontainers) | 2 | Docker + ODBC 18 on host, `-m integration` |
| Postgres → Postgres integration (testcontainers) | 1 | Docker, `-m integration` |

### Live verified end-to-end

- **MSSQL → Postgres** via docker-compose: 5 Users / 4 Posts / 2 Comments /
  2 PostTypes / 2 VoteTypes / 2 LinkTypes land in
  `dev_stackoverflow2010_dbo`. Task SUCCESS in ~500ms.
- **Postgres → Postgres** via testcontainers: 3 customers + 4 orders land
  in `test_app_app`. Total wall time ~4s including container boot.

---

## [#3 — Single parameterized DAG](https://github.com/johndauphine/airflow-dlt/pull/3) (squashed as `5d45b7a`)

Reverted from "one DAG per YAML" (factory pattern) back to **one DAG that
picks the YAML at trigger time** via `{"config_name": "<yaml stem>"}`. The
factory pattern crept in during PR #1's iteration to make per-YAML
`schedule` / `retries` fields actually take effect; the cost was N DAGs
appearing in the Airflow UI when the original intent was one. This PR
collapsed the surface area back without re-introducing the inert-fields
footgun: `pipeline.schedule` and friends are now strictly rejected by
schema validation. Scheduling lives on the DAG, not in YAML.

### Highlights

- `PipelineMeta` carries only `name`. `schedule` / `retries` /
  `retry_delay_seconds` / `max_active_runs` are now `ValidationError`s.
- New [`dags/example_scheduled.py`](dags/example_scheduled.py) template
  for production-scheduled pipelines. Ships **paused** so cloning the
  repo doesn't auto-fire hourly loads.
- Resolver in `dlt_pipeline.py` has a traversal guard and lists
  available configs on miss.
- `run()` task uses `get_current_context()["params"]` explicitly instead
  of relying on Airflow's `@task` auto-injection of `params: dict`.
  Same behavior; self-documenting and survives static analyzers (raised
  by Copilot's PR review).

### Operational change

To run any pipeline:
```
Trigger DAG: dlt_pipeline
Conf:        {"config_name": "stackoverflow"}
```

To schedule a specific pipeline: copy `dags/example_scheduled.py`,
rename the `dag_id`, set the `CONFIG_PATH` and `schedule`, unpause.

---

## [#2 — Generic source/target via discriminated unions + connector registry](https://github.com/johndauphine/airflow-dlt/pull/2) (squashed as `0cb7e81`)

Made source and target configurable per pipeline via YAML. Introduced a
connector framework: `SourceConnector` / `TargetConnector` ABCs with a
module-level registry dispatched by `cfg.source.type` / `cfg.target.type`.
Pydantic discriminated unions per `type` mean each endpoint variant gets
its own validated schema (MSSQL needs `driver` + `options`, Postgres needs
`sslmode`, SQLite needs only `path`).

### Highlights

- **MSSQL, Postgres, SQLite** sources registered.
- **Postgres, SQLite** targets registered. (Snowflake schema/connector
  deferred — connector framework supports it; registering is one new file
  in a future PR.)
- `derive_dataset_name` accepts `source_schema=None` (schemaless backends
  like SQLite drop the trailing segment) and uses a single underscore
  between segments. The single-underscore choice matches dlt's actual
  postgres destination normalization — caught while running the
  Postgres→Postgres integration test live: dlt was silently collapsing
  `test__app__public` → `test_app_public` at write time, so our
  `pipeline.dataset_name` had been diverging from the real schema.
- New `tests/integration/test_postgres_to_postgres.py` exercises the new
  source connector against a second `PostgresContainer`.
- **`tests/integration/test_sqlite_smoke.py`** — full end-to-end load
  using stdlib `sqlite3` and dlt's `sqlalchemy` destination. **No
  Docker, no marker**: runs in the default test suite. CI regression
  guard that needs no infra.

### Operational notes

- `dlt.destinations.sqlalchemy` for SQLite writes data into a sibling file
  named `{target_stem}__{dataset_name}.db` next to the configured target
  path — the configured target file is a state anchor. The smoke test
  finds the data file by globbing.
- The Airflow image now bundles every supported driver; one heavy image
  for all deployments (no per-environment driver builds).

---

## [#1 — Initial scaffold](https://github.com/johndauphine/airflow-dlt/pull/1) (squashed as `a4aea96`)

DLT-first reimagining of the
[`mssql-to-postgres-pipeline`](https://github.com/johndauphine/mssql-to-postgres-pipeline)
template. Initial scaffold + 5 rounds of codex review fixes.

### What landed

- **Pydantic config schema** with `extra="forbid"` on every model.
- **`SecretsClient` interface** with `MockDelineaClient` (YAML-backed)
  implementation. Real Delinea plugs in by implementing one method.
- **Schema naming** via hostname alias: `{alias}_{db}_{schema}`.
- **Single parameterized DAG** (later refactored to a factory, then
  reverted in PR #3).
- **docker-compose** stack: Airflow 3.0 + MSSQL 2022 + `mssql-init`
  one-shot seed + Postgres 16, all on one docker network with the
  ODBC 18 driver baked into the Airflow image.
- **Example pipeline** (`config/pipelines/stackoverflow.yaml`) replicating
  the StackOverflow2010 tables from the template repo.
- **17 plugin unit tests + DAG integrity tests** + a testcontainers
  integration test for MSSQL → Postgres.
- **README + CLAUDE.md** explaining architecture and trade-offs.

### Notable bug fixes hammered out during the codex iteration cycle

1. **`quote_plus` → `quote(safe="")`** for Postgres URL userinfo —
   SQLAlchemy parses URL userinfo per RFC 3986 where `+` is a literal,
   so `quote_plus` silently corrupted credentials containing spaces.
2. **`dlt-state` Docker volume permissions** — the named volume was
   root-owned but Airflow runs as UID 50000. Mounted it in
   `airflow-init` and chowned it there.
3. **Per-file DAG-construction errors contained.** Invalid YAML or bad
   cron used to take down the whole module's parse and hide every
   pipeline; now each bad file becomes a `dlt_broken__*` DAG.
4. **Duplicate `pipeline.name`** in two YAMLs used to silently collapse
   into one DAG; now the second one registers as broken.
5. **`auto_register=False`** on the `@dag` decorator — without it,
   invalid DAGs leaked into `DagContext.autoregistered_dags` even when
   excluded from `globals()`, defeating the broken-DAG pattern.
6. **`secrets.py` → `secrets_client.py`.** Airflow's plugin loader puts
   package subdirs on `sys.path`, so our `secrets.py` was shadowing
   stdlib `secrets`. numpy's Cython init does
   `from secrets import randbits`, which then failed with the cryptic
   `ImportError: cannot import name randbits`. Took a docker-compose
   run to surface this one.
7. **`sql_database` wants a string**, not a SQLAlchemy `URL` object.
   Now rendered via `url.render_as_string(hide_password=False)`.
8. **Lazy-import `build_pipeline`** inside the task body. Importing dlt
   at DAG file top tripped a numpy/pyarrow Cython init-order bug under
   DagBag's parse subprocess. Same import works fine at task runtime.

---

## Deferred — not in `main` yet

| | Why |
|---|---|
| Snowflake target connector | Framework supports it; registering is one new file. Planned for next round. |
| BigQuery / MySQL endpoints | Same pattern; not in MVP scope. |
| Real Delinea integration | `SecretsClient` interface is the seam; mock works for dev/CI until corporate gives credentials. |
| Notification module | Airflow 3.0 callback gaps make it not worth building yet. |
| Airflow secrets-log noise suppression | Cosmetic — same `"Secrets backends loaded for worker"` line repeats ~900x per task run. Fixed in Airflow 3.1+; bump deferred until we know the deployment target's version constraint. |

## Known issues

- **Airflow UI flashes in Safari** — cookie/auth redirect loop. Use Chrome,
  or `http://127.0.0.1:8080` instead of `http://localhost:8080`.
- **MSSQL container on Apple Silicon** runs under Rosetta emulation; SQL
  Server boot is slow but works.
- **Pinned to `apache-airflow==3.0.0`**; lots of noisy info-level logging
  in worker task output. Fixed upstream by 3.2.x.
