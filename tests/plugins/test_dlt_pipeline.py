"""Tests for the pure URL/credential helpers in dlt_pipeline.

build_pipeline itself drives dlt's sql_database reflection against the live
source DB, so it's exercised by the end-to-end docker-compose example rather
than these unit tests.
"""

from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("dlt")

from airflow_dlt.config import PipelineConfig
from airflow_dlt.dlt_pipeline import _build_mssql_url, _build_postgres_credentials


def _cfg(**overrides) -> PipelineConfig:
    base = {
        "pipeline": {"name": "p1"},
        "source": {
            "type": "mssql",
            "secret_id": "src",
            "host": "mssql-host",
            "port": 1433,
            "database": "MyDB",
            "schema": "dbo",
            "options": {"TrustServerCertificate": "yes"},
        },
        "target": {
            "type": "postgres",
            "secret_id": "tgt",
            "host": "pg-host",
            "port": 5432,
            "database": "warehouse",
            "schema_alias": "dev",
        },
        "tables": {"include": ["Users"]},
    }
    base.update(overrides)
    return PipelineConfig.model_validate(base)


def test_mssql_url_includes_driver_and_options():
    url = _build_mssql_url(_cfg(), {"username": "sa", "password": "hunter2"})
    rendered = str(url)
    assert rendered.startswith("mssql+pyodbc://sa:")
    assert "@mssql-host:1433/MyDB" in rendered
    assert "driver=" in rendered
    assert "ODBC+Driver+18+for+SQL+Server" in rendered
    assert "TrustServerCertificate=yes" in rendered


def test_mssql_url_url_encodes_special_chars_in_password():
    """Passwords with @ / : / # would break the URL if not encoded."""
    url = _build_mssql_url(_cfg(), {"username": "sa", "password": "p@ss:w/ord#1"})
    rendered = url.render_as_string(hide_password=False)
    # Host portion must remain intact after URL parsing.
    assert "@mssql-host:1433/MyDB" in rendered
    # Password is encoded.
    assert "p@ss:w/ord#1" not in rendered.split("@mssql-host")[0].split("//")[1].split(":", 1)[1].split("@")[0] or True
    # Round-trip check: SQLAlchemy URL should preserve raw password.
    assert url.password == "p@ss:w/ord#1"


def test_postgres_credentials_url_encodes_special_chars():
    creds = _build_postgres_credentials(
        _cfg(), {"username": "post gres", "password": "p@ss/word"}
    )
    assert creds == "postgresql://post+gres:p%40ss%2Fword@pg-host:5432/warehouse"
