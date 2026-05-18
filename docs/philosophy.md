# Philosophy

This repository is intentionally small glue around mature systems. Airflow
owns orchestration, dlt owns extraction/loading/state, Postgres and SQL Server
own storage, and YAML owns pipeline intent.

## Prefer Boring Boundaries

The project started from the observation that a hand-written migration
pipeline can grow thousands of lines of custom machinery: schema extraction,
type mapping, DDL generation, binary copy, retries, state tables, and progress
tracking. Most of that is not business logic. It is infrastructure code that
already has better homes.

This repo keeps only the pieces that are specific to this deployment:

- How to validate a pipeline YAML file.
- How to resolve a `secret_id`.
- How to turn source/target config into connector URLs.
- How to derive stable dataset names.
- How to expose the run through Airflow.

Everything else should be delegated unless the repo has a clear reason to own
it.

## One Manual DAG, Many Pipeline Configs

The manual runner is a single Airflow DAG: `dlt_pipeline`.

Each YAML under `config/pipelines/` defines what to move. The DAG defines how
the work is materialized in Airflow. A multi-table YAML creates one mapped task
per table in one DAG run. Airflow pool slots decide how many table loads run
at once.

This keeps operational questions in Airflow:

- How many table loads may run concurrently?
- How many DAG runs may overlap?
- How do retries behave?
- What is scheduled?

And it keeps pipeline questions in YAML:

- Which source?
- Which target?
- Which schema?
- Which tables?
- Which dlt write disposition and hints?

## Configuration Is Explicit

The YAML schema is strict. Unknown keys fail validation. Endpoint-specific
fields live only on the endpoint type that understands them.

The goal is to make mistakes loud. A typo in a migration config should fail
before the data load begins.

## Secrets Are References

Pipeline YAML files should not contain passwords. They reference `secret_id`
values. The local implementation reads `config/secrets.yaml`; a production
implementation can use Delinea or another secret manager behind the same
`SecretsClient` interface.

That boundary lets local development stay easy without changing the pipeline
format when secrets move to a real vault.

## Use dlt State Before Inventing State

dlt already tracks pipeline state and load package state. This repo should not
reintroduce a parallel migration-state subsystem unless there is a concrete
case dlt cannot handle.

For repeat runs, prefer dlt-native concepts:

- `replace` for baseline reloads.
- `append` for append-only flows.
- `merge` with `primary_key` for upserts.
- `incremental` with a cursor for cheap subsequent extraction.

Plain `merge` without an incremental cursor still requires dlt to re-extract
the source.

## Benchmark The Path, Not Just The Database

The important performance unit is the whole path:

source database -> dlt extraction -> local package files -> normalization ->
destination loader -> target database.

That is why benchmark notes include DAG runtime, dlt load timing, row counts,
working directory size, and container pressure. A fast database copy method is
not enough if extraction or staging dominates the run.

## Keep The Repo Friendly To The Next Agent

This repo is likely to be operated by both humans and AI coding agents. Good
documentation is part of the runtime. When behavior changes, update the docs
that explain the operator model, not just the code.

Useful docs should answer:

- What command do I run first?
- What file owns this decision?
- What must not be renamed?
- What benchmark result should I compare against?
- What failure mode have we already learned from?
