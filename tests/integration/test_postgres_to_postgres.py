"""L1 smoke test for Postgres → Postgres migration.

Verifies the new generic source path: build_pipeline goes through
PostgresSource (sqlalchemy_url) → dlt's sql_database → PostgresTarget → live
Postgres. Cheaper than the MSSQL test (no Rosetta emulation needed).
"""

from __future__ import annotations

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("psycopg2")
pytest.importorskip("dlt")

from airflow_dlt.config import PipelineConfig
from airflow_dlt.dlt_pipeline import build_pipeline
from airflow_dlt.schema_naming import derive_dataset_name
from airflow_dlt.secrets_client import SecretsClient

pytestmark = pytest.mark.integration


class _DictSecrets(SecretsClient):
    def __init__(self, mapping: dict[str, dict[str, str]]):
        self._mapping = mapping

    def get(self, secret_id: str) -> dict[str, str]:
        return dict(self._mapping[secret_id])


def _seed_source(conn) -> None:
    """Create app schema + two demo tables in the source Postgres."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS app")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app.customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                signup_at TIMESTAMP NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app.orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                total NUMERIC(10,2) NOT NULL
            )
        """)
        cur.execute("TRUNCATE app.customers, app.orders")
        cur.executemany(
            "INSERT INTO app.customers (id, name, signup_at) VALUES (%s, %s, %s)",
            [
                (1, "alice", "2026-01-01"),
                (2, "bob",   "2026-02-01"),
                (3, "carol", "2026-03-01"),
            ],
        )
        cur.executemany(
            "INSERT INTO app.orders (id, customer_id, total) VALUES (%s, %s, %s)",
            [
                (1, 1, 19.99),
                (2, 1, 4.50),
                (3, 2, 100.00),
                (4, 3, 7.25),
            ],
        )


def _pipeline_config(src, tgt, *, pipeline_name: str) -> PipelineConfig:
    return PipelineConfig.model_validate({
        "pipeline": {"name": pipeline_name},
        "source": {
            "type": "postgres",
            "secret_id": "src",
            "host": str(src["host"]),
            "port": int(src["port"]),
            "database": str(src["database"]),
            "schema": "app",
        },
        "target": {
            "type": "postgres",
            "secret_id": "tgt",
            "host": str(tgt["host"]),
            "port": int(tgt["port"]),
            "database": str(tgt["database"]),
            "schema_alias": "test",
        },
        "tables": {"include": ["customers", "orders"]},
        "load": {"write_disposition": "replace", "chunk_size": 1000},
    })


def _row_count(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]


def test_postgres_to_postgres_replace(
    postgres_source_admin_conn,
    postgres_admin_conn,
    postgres_source_endpoint,
    postgres_endpoint,
):
    """End-to-end: seeded rows in source Postgres land in target Postgres."""
    _seed_source(postgres_source_admin_conn)

    secrets = _DictSecrets({
        "src": {
            "username": postgres_source_endpoint["username"],
            "password": postgres_source_endpoint["password"],
        },
        "tgt": {
            "username": postgres_endpoint["username"],
            "password": postgres_endpoint["password"],
        },
    })
    cfg = _pipeline_config(
        postgres_source_endpoint, postgres_endpoint, pipeline_name="smoke_pg_to_pg"
    )
    pipeline, source = build_pipeline(cfg, secrets)
    load_info = pipeline.run(source)
    assert not load_info.has_failed_jobs, f"dlt reported failed jobs: {load_info}"

    dataset = derive_dataset_name("test", postgres_source_endpoint["database"], "app")
    assert _row_count(postgres_admin_conn, dataset, "customers") == 3
    assert _row_count(postgres_admin_conn, dataset, "orders") == 4

    with postgres_admin_conn.cursor() as cur:
        cur.execute(f'SELECT id, name FROM "{dataset}".customers ORDER BY id')
        assert cur.fetchall() == [(1, "alice"), (2, "bob"), (3, "carol")]
