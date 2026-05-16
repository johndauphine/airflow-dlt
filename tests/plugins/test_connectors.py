"""Tests for source/target connectors.

URL construction is deterministic and easy to test in isolation; the live-DB
parts (sql_database reflection, dlt.pipeline.run) are covered by the
testcontainers integration tests.
"""

from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("dlt")

from airflow_dlt.config import MssqlSourceCfg, PostgresSourceCfg, PostgresTargetCfg
from airflow_dlt.connectors import (
    MssqlSource,
    PostgresSource,
    PostgresTarget,
    make_source_connector,
    make_target_connector,
)


# ---------------------------------------------------------------------------
# MSSQL source
# ---------------------------------------------------------------------------

def _mssql_cfg(**overrides) -> MssqlSourceCfg:
    base = {
        "type": "mssql",
        "secret_id": "src",
        "host": "mssql-host",
        "port": 1433,
        "database": "MyDB",
        "schema": "dbo",
        "options": {"TrustServerCertificate": "yes"},
    }
    base.update(overrides)
    return MssqlSourceCfg.model_validate(base)


def test_mssql_url_includes_driver_and_options():
    url = MssqlSource(_mssql_cfg()).sqlalchemy_url(
        {"username": "sa", "password": "hunter2"}
    )
    assert url.startswith("mssql+pyodbc://sa:")
    assert "@mssql-host:1433/MyDB" in url
    assert "driver=" in url
    assert "ODBC+Driver+18+for+SQL+Server" in url
    assert "TrustServerCertificate=yes" in url


def test_mssql_password_with_special_chars_round_trips():
    url = MssqlSource(_mssql_cfg()).sqlalchemy_url(
        {"username": "sa", "password": "p@ss:w/ord#1"}
    )
    # Host portion must remain intact after URL parsing.
    assert "@mssql-host:1433/MyDB" in url


def test_mssql_database_and_schema():
    src = MssqlSource(_mssql_cfg(database="OtherDB", schema="audit"))
    assert src.database_name() == "OtherDB"
    assert src.schema_name() == "audit"


# ---------------------------------------------------------------------------
# Postgres source
# ---------------------------------------------------------------------------

def _pg_source_cfg(**overrides) -> PostgresSourceCfg:
    base = {
        "type": "postgres",
        "secret_id": "src",
        "host": "pg-source",
        "port": 5432,
        "database": "app",
        "schema": "public",
    }
    base.update(overrides)
    return PostgresSourceCfg.model_validate(base)


def test_postgres_source_url_default():
    url = PostgresSource(_pg_source_cfg()).sqlalchemy_url(
        {"username": "reader", "password": "hunter2"}
    )
    assert url.startswith("postgresql+psycopg2://reader:")
    assert "@pg-source:5432/app" in url


def test_postgres_source_url_with_sslmode():
    url = PostgresSource(_pg_source_cfg(sslmode="require")).sqlalchemy_url(
        {"username": "u", "password": "p"}
    )
    assert "sslmode=require" in url


def test_postgres_source_database_and_schema():
    src = PostgresSource(_pg_source_cfg(database="shop", schema="orders"))
    assert src.database_name() == "shop"
    assert src.schema_name() == "orders"


# ---------------------------------------------------------------------------
# Postgres target
# ---------------------------------------------------------------------------

def _pg_target_cfg(**overrides) -> PostgresTargetCfg:
    base = {
        "type": "postgres",
        "secret_id": "tgt",
        "host": "pg-host",
        "port": 5432,
        "database": "warehouse",
        "schema_alias": "dev",
    }
    base.update(overrides)
    return PostgresTargetCfg.model_validate(base)


def test_postgres_target_connection_string_url_encodes_special_chars():
    tgt = PostgresTarget(_pg_target_cfg())
    cs = tgt._connection_string({"username": "post gres", "password": "p@ss/word"})
    # RFC 3986 percent-encoding: space → %20 (not +) — SQLAlchemy reads + as
    # a literal in userinfo, so quote_plus would silently corrupt creds.
    assert cs == "postgresql://post%20gres:p%40ss%2Fword@pg-host:5432/warehouse"


def test_postgres_target_plus_sign_preserved():
    tgt = PostgresTarget(_pg_target_cfg())
    cs = tgt._connection_string({"username": "u", "password": "a+b"})
    assert cs == "postgresql://u:a%2Bb@pg-host:5432/warehouse"


def test_postgres_target_sslmode_query_added():
    tgt = PostgresTarget(_pg_target_cfg(sslmode="require"))
    cs = tgt._connection_string({"username": "u", "password": "p"})
    assert cs.endswith("?sslmode=require")


# ---------------------------------------------------------------------------
# Registry dispatch
# ---------------------------------------------------------------------------

def test_registry_dispatch_source():
    assert isinstance(make_source_connector(_mssql_cfg()), MssqlSource)
    assert isinstance(make_source_connector(_pg_source_cfg()), PostgresSource)


def test_registry_dispatch_target():
    assert isinstance(make_target_connector(_pg_target_cfg()), PostgresTarget)
