"""Fixtures for tests under tests/integration/.

Some tests in here need Docker + testcontainers (the MSSQL→Postgres and
Postgres→Postgres tests); others don't (the SQLite smoke test). Gate the
optional imports per-fixture so the conftest itself always loads — otherwise
a missing testcontainers install would also hide the no-infra SQLite tests.
"""

from __future__ import annotations

import os
import time
from typing import Iterator

import pytest

try:
    from testcontainers.mssql import SqlServerContainer  # type: ignore
    from testcontainers.postgres import PostgresContainer  # type: ignore
    _TC_AVAILABLE = True
except ImportError:
    _TC_AVAILABLE = False
    SqlServerContainer = PostgresContainer = None  # type: ignore[assignment]

try:
    import pyodbc  # type: ignore
    _PYODBC_AVAILABLE = True
except ImportError:
    _PYODBC_AVAILABLE = False

try:
    import psycopg2  # type: ignore
    _PSYCOPG_AVAILABLE = True
except ImportError:
    _PSYCOPG_AVAILABLE = False


MSSQL_IMAGE = os.environ.get(
    "TEST_MSSQL_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest"
)
PG_IMAGE = os.environ.get("TEST_PG_IMAGE", "postgres:16-alpine")
MSSQL_PASSWORD = "Strong!Passw0rd_2026"


def _require_tc():
    if not _TC_AVAILABLE:
        pytest.skip("testcontainers not installed (`uv sync --extra integration`)")


def _require_pyodbc():
    if not _PYODBC_AVAILABLE:
        pytest.skip("pyodbc + ODBC Driver 18 required on host")


def _require_psycopg():
    if not _PSYCOPG_AVAILABLE:
        pytest.skip("psycopg2 not installed (`uv sync --extra integration`)")


@pytest.fixture(scope="session")
def mssql_container() -> Iterator:
    _require_tc()
    _require_pyodbc()
    container = (
        SqlServerContainer(MSSQL_IMAGE, password=MSSQL_PASSWORD)
        .with_env("ACCEPT_EULA", "Y")
        .with_env("MSSQL_PID", "Developer")
    )
    container.start()
    try:
        _wait_for_mssql(container, timeout_seconds=120)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgres_container() -> Iterator:
    """Target Postgres. Re-used across tests."""
    _require_tc()
    container = PostgresContainer(PG_IMAGE, username="pguser", password="pgpass", dbname="warehouse")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgres_source_container() -> Iterator:
    """Distinct Postgres instance used as a source for pg→pg tests."""
    _require_tc()
    container = PostgresContainer(PG_IMAGE, username="srcuser", password="srcpass", dbname="app")
    container.start()
    try:
        yield container
    finally:
        container.stop()


def _wait_for_mssql(container, timeout_seconds: int) -> None:
    """SqlServerContainer.start() returns before SQL Server accepts logins."""
    deadline = time.time() + timeout_seconds
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pyodbc.connect(_pyodbc_dsn(container), timeout=5)
            conn.close()
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"MSSQL container never accepted logins: {last_err!r}")


def _pyodbc_dsn(container) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(1433)
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};"
        "DATABASE=master;"
        f"UID=sa;PWD={MSSQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )


@pytest.fixture
def mssql_admin_conn(mssql_container):
    """Fresh pyodbc connection to the MSSQL master DB for test setup."""
    conn = pyodbc.connect(_pyodbc_dsn(mssql_container), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def postgres_admin_conn(postgres_container):
    """Fresh psycopg2 connection for assertions against the target."""
    _require_psycopg()
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user="pguser",
        password="pgpass",
        dbname="warehouse",
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def postgres_source_admin_conn(postgres_source_container):
    """Admin connection to seed the source Postgres for pg→pg tests."""
    _require_psycopg()
    conn = psycopg2.connect(
        host=postgres_source_container.get_container_host_ip(),
        port=postgres_source_container.get_exposed_port(5432),
        user="srcuser",
        password="srcpass",
        dbname="app",
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def mssql_endpoint(mssql_container) -> dict[str, object]:
    return {
        "host": mssql_container.get_container_host_ip(),
        "port": int(mssql_container.get_exposed_port(1433)),
        "username": "sa",
        "password": MSSQL_PASSWORD,
    }


@pytest.fixture
def postgres_endpoint(postgres_container) -> dict[str, object]:
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "username": "pguser",
        "password": "pgpass",
        "database": "warehouse",
    }


@pytest.fixture
def postgres_source_endpoint(postgres_source_container) -> dict[str, object]:
    return {
        "host": postgres_source_container.get_container_host_ip(),
        "port": int(postgres_source_container.get_exposed_port(5432)),
        "username": "srcuser",
        "password": "srcpass",
        "database": "app",
    }
