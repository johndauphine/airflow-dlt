"""L1 smoke test: prove pipeline.run() actually moves rows MSSQL → Postgres.

Drives ``build_pipeline`` directly against testcontainers-managed MSSQL and
Postgres instances. Tests the same code path the DAG would, minus Airflow.
"""

from __future__ import annotations

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("pyodbc")
pytest.importorskip("psycopg2")
pytest.importorskip("dlt")

from airflow_dlt.config import PipelineConfig
from airflow_dlt.dlt_pipeline import build_pipeline
from airflow_dlt.schema_naming import derive_dataset_name
from airflow_dlt.secrets_client import SecretsClient

pytestmark = pytest.mark.integration


class _DictSecrets(SecretsClient):
    """In-process SecretsClient backed by a dict — for tests only."""

    def __init__(self, mapping: dict[str, dict[str, str]]):
        self._mapping = mapping

    def get(self, secret_id: str) -> dict[str, str]:
        return dict(self._mapping[secret_id])


def _seed_demo_db(conn, *, n_users: int = 5, n_posts: int = 7) -> None:
    """Create StackOverflow2010 + dbo.Users + dbo.Posts and insert known rows."""
    cur = conn.cursor()
    cur.execute("IF DB_ID('StackOverflow2010') IS NULL CREATE DATABASE StackOverflow2010")
    cur.execute("USE StackOverflow2010")
    cur.execute("""
        IF OBJECT_ID('dbo.Users', 'U') IS NULL
        CREATE TABLE dbo.Users (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            DisplayName NVARCHAR(40) NOT NULL,
            Reputation INT NOT NULL
        )
    """)
    cur.execute("""
        IF OBJECT_ID('dbo.Posts', 'U') IS NULL
        CREATE TABLE dbo.Posts (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            OwnerUserId INT NOT NULL,
            Title NVARCHAR(200) NOT NULL
        )
    """)
    cur.execute("TRUNCATE TABLE dbo.Users")
    cur.execute("TRUNCATE TABLE dbo.Posts")
    for i in range(n_users):
        cur.execute(
            "INSERT INTO dbo.Users (DisplayName, Reputation) VALUES (?, ?)",
            f"user_{i}", i * 10,
        )
    for i in range(n_posts):
        cur.execute(
            "INSERT INTO dbo.Posts (OwnerUserId, Title) VALUES (?, ?)",
            (i % n_users) + 1, f"post_{i}",
        )


def _pipeline_config(mssql, pg, pipeline_name: str) -> PipelineConfig:
    return PipelineConfig.model_validate({
        "pipeline": {"name": pipeline_name},
        "source": {
            "type": "mssql",
            "secret_id": "src",
            "host": str(mssql["host"]),
            "port": int(mssql["port"]),
            "database": "StackOverflow2010",
            "schema": "dbo",
            "driver": "ODBC Driver 18 for SQL Server",
            "options": {"TrustServerCertificate": "yes"},
        },
        "target": {
            "type": "postgres",
            "secret_id": "tgt",
            "host": str(pg["host"]),
            "port": int(pg["port"]),
            "database": str(pg["database"]),
            "schema_alias": "test",
        },
        "tables": {"include": ["Users", "Posts"]},
        "load": {"write_disposition": "replace", "chunk_size": 1000},
    })


def _row_count(pg_conn, schema: str, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]


def test_replace_disposition_moves_seeded_rows(
    mssql_admin_conn, postgres_admin_conn, mssql_endpoint, postgres_endpoint
):
    """Smoke test: a full replace load lands every seeded row in Postgres."""
    _seed_demo_db(mssql_admin_conn, n_users=5, n_posts=7)

    secrets = _DictSecrets({
        "src": {"username": mssql_endpoint["username"], "password": mssql_endpoint["password"]},
        "tgt": {"username": postgres_endpoint["username"], "password": postgres_endpoint["password"]},
    })
    cfg = _pipeline_config(mssql_endpoint, postgres_endpoint, "smoke_replace")
    pipeline, source = build_pipeline(cfg, secrets)
    load_info = pipeline.run(source)
    assert not load_info.has_failed_jobs, f"dlt load reported failed jobs: {load_info}"

    dataset = derive_dataset_name("test", "StackOverflow2010", "dbo")
    assert _row_count(postgres_admin_conn, dataset, "users") == 5
    assert _row_count(postgres_admin_conn, dataset, "posts") == 7

    # Spot-check a couple of values to prove this is real data movement,
    # not just empty tables created at the destination.
    with postgres_admin_conn.cursor() as cur:
        cur.execute(
            f'SELECT display_name, reputation FROM "{dataset}".users ORDER BY id'
        )
        rows = cur.fetchall()
    assert rows == [(f"user_{i}", i * 10) for i in range(5)]


def test_replace_disposition_truncates_prior_load(
    mssql_admin_conn, postgres_admin_conn, mssql_endpoint, postgres_endpoint
):
    """Second run with `replace` must not double-write or retain stale rows."""
    _seed_demo_db(mssql_admin_conn, n_users=3, n_posts=0)

    secrets = _DictSecrets({
        "src": {"username": mssql_endpoint["username"], "password": mssql_endpoint["password"]},
        "tgt": {"username": postgres_endpoint["username"], "password": postgres_endpoint["password"]},
    })
    cfg = _pipeline_config(mssql_endpoint, postgres_endpoint, "smoke_replace_2")

    pipeline, source = build_pipeline(cfg, secrets)
    pipeline.run(source)

    # Reseed with a *different* row count.
    _seed_demo_db(mssql_admin_conn, n_users=2, n_posts=0)
    pipeline2, source2 = build_pipeline(cfg, secrets)
    pipeline2.run(source2)

    dataset = derive_dataset_name("test", "StackOverflow2010", "dbo")
    assert _row_count(postgres_admin_conn, dataset, "users") == 2
