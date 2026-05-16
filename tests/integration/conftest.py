"""Fixtures for testcontainers-driven integration tests.

These spin up real MSSQL + Postgres containers via Docker. They share session
scope so a full integration run pays the ~30s SQL Server boot once.

Mark the whole module with @pytest.mark.integration so `pytest` (default
addopts: `-m 'not integration'`) doesn't pull these in unintentionally.
"""

from __future__ import annotations

import os
import time
from typing import Iterator

import pytest

testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")
pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed")
psycopg2 = pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from testcontainers.mssql import SqlServerContainer  # noqa: E402 - must follow importorskip
from testcontainers.postgres import PostgresContainer  # noqa: E402 - must follow importorskip


MSSQL_IMAGE = os.environ.get(
    "TEST_MSSQL_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest"
)
PG_IMAGE = os.environ.get("TEST_PG_IMAGE", "postgres:16-alpine")
MSSQL_PASSWORD = "Strong!Passw0rd_2026"


@pytest.fixture(scope="session")
def mssql_container() -> Iterator[SqlServerContainer]:
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
def postgres_container() -> Iterator[PostgresContainer]:
    container = PostgresContainer(PG_IMAGE, username="pguser", password="pgpass", dbname="warehouse")
    container.start()
    try:
        yield container
    finally:
        container.stop()


def _wait_for_mssql(container: SqlServerContainer, timeout_seconds: int) -> None:
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


def _pyodbc_dsn(container: SqlServerContainer) -> str:
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
