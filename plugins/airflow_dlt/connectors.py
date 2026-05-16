"""Source and target connectors.

A connector owns the type-specific bits of a pipeline: how to turn a YAML
config + credentials dict into a SQLAlchemy URL (sources) or a dlt
destination (targets).

Adding a new source/target type:
  1. Add a Cfg model to ``config.py`` and put it in the discriminated union.
  2. Add a connector class here that subclasses Source/TargetConnector.
  3. Register the class in SOURCE_CONNECTORS / TARGET_CONNECTORS.

The rest of the pipeline (build_pipeline, the DAG factory, the secrets
client) is type-agnostic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import quote

import dlt
from sqlalchemy.engine.url import URL

from pathlib import Path

from airflow_dlt.config import (
    MssqlSourceCfg,
    PostgresSourceCfg,
    PostgresTargetCfg,
    SourceConfig,
    SqliteSourceCfg,
    SqliteTargetCfg,
    TargetConfig,
)


# ===========================================================================
# Source connectors
# ===========================================================================

class SourceConnector(ABC):
    """One concrete class per supported source type (mssql, postgres, ...)."""

    @abstractmethod
    def sqlalchemy_url(self, creds: dict[str, str]) -> str:
        """Return a connection string usable by dlt's ``sql_database`` source."""

    @abstractmethod
    def database_name(self) -> str:
        """Source database name (used in target dataset naming)."""

    @abstractmethod
    def schema_name(self) -> str | None:
        """Source schema name, or None for schemaless backends (MySQL, SQLite)."""


class MssqlSource(SourceConnector):
    def __init__(self, cfg: MssqlSourceCfg) -> None:
        self.cfg = cfg

    def sqlalchemy_url(self, creds: dict[str, str]) -> str:
        query: dict[str, str] = {"driver": self.cfg.driver, **self.cfg.options}
        url = URL.create(
            drivername="mssql+pyodbc",
            username=creds["username"],
            password=creds["password"],
            host=self.cfg.host,
            port=self.cfg.port,
            database=self.cfg.database,
            query=query,
        )
        # dlt's sql_database wants a connection-string str, not a URL object.
        return url.render_as_string(hide_password=False)

    def database_name(self) -> str:
        return self.cfg.database

    def schema_name(self) -> str | None:
        return self.cfg.schema_


class PostgresSource(SourceConnector):
    def __init__(self, cfg: PostgresSourceCfg) -> None:
        self.cfg = cfg

    def sqlalchemy_url(self, creds: dict[str, str]) -> str:
        query: dict[str, str] = dict(self.cfg.options)
        if self.cfg.sslmode is not None:
            query.setdefault("sslmode", self.cfg.sslmode)
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=creds["username"],
            password=creds["password"],
            host=self.cfg.host,
            port=self.cfg.port,
            database=self.cfg.database,
            query=query,
        )
        return url.render_as_string(hide_password=False)

    def database_name(self) -> str:
        return self.cfg.database

    def schema_name(self) -> str | None:
        return self.cfg.schema_


class SqliteSource(SourceConnector):
    """SQLite source. Uses the file path as the database identifier."""

    def __init__(self, cfg: SqliteSourceCfg) -> None:
        self.cfg = cfg

    def sqlalchemy_url(self, creds: dict[str, str]) -> str:
        # creds intentionally unused — SQLite has no auth.
        # Absolute paths produce sqlite:////abs/path (4 slashes); relative
        # paths produce sqlite:///rel/path (3 slashes). The f-string yields
        # both correctly since absolute paths already start with '/'.
        return f"sqlite:///{self.cfg.path}"

    def database_name(self) -> str:
        # Use the file stem so dataset names stay readable
        # (e.g. /data/source.db → "source").
        path = self.cfg.path
        if path == ":memory:":
            return "memory"
        return Path(path).stem or "sqlite"

    def schema_name(self) -> str | None:
        return None


# ===========================================================================
# Target connectors
# ===========================================================================

class TargetConnector(ABC):
    """One concrete class per supported destination type (postgres, ...)."""

    @abstractmethod
    def build_destination(self, creds: dict[str, str]) -> Any:
        """Return a dlt destination ready to be passed to dlt.pipeline()."""


class PostgresTarget(TargetConnector):
    def __init__(self, cfg: PostgresTargetCfg) -> None:
        self.cfg = cfg

    def build_destination(self, creds: dict[str, str]) -> Any:
        return dlt.destinations.postgres(credentials=self._connection_string(creds))

    def _connection_string(self, creds: dict[str, str]) -> str:
        """RFC 3986 percent-encoded postgres URL.

        NOTE: never use ``urllib.parse.quote_plus`` here — SQLAlchemy parses URL
        userinfo per RFC 3986 where ``+`` is a literal, so quote_plus silently
        corrupts credentials containing spaces.
        """
        user = quote(creds["username"], safe="")
        pw = quote(creds["password"], safe="")
        query = f"?sslmode={quote(self.cfg.sslmode, safe='')}" if self.cfg.sslmode else ""
        return (
            f"postgresql://{user}:{pw}@{self.cfg.host}:{self.cfg.port}/"
            f"{self.cfg.database}{query}"
        )


class SqliteTarget(TargetConnector):
    """SQLite target via dlt's sqlalchemy destination.

    Useful for CI smoke tests where spinning up a Postgres/MSSQL container is
    overkill. dlt writes tables directly into the target SQLite file.
    """

    def __init__(self, cfg: SqliteTargetCfg) -> None:
        self.cfg = cfg

    def build_destination(self, creds: dict[str, str]) -> Any:
        # creds intentionally unused — SQLite has no auth.
        return dlt.destinations.sqlalchemy(credentials=f"sqlite:///{self.cfg.path}")


# ===========================================================================
# Registries — used by build_pipeline to dispatch on cfg.source.type / cfg.target.type
# ===========================================================================

SOURCE_CONNECTORS: dict[str, type[SourceConnector]] = {
    "mssql": MssqlSource,
    "postgres": PostgresSource,
    "sqlite": SqliteSource,
}

TARGET_CONNECTORS: dict[str, type[TargetConnector]] = {
    "postgres": PostgresTarget,
    "sqlite": SqliteTarget,
}


def make_source_connector(cfg: SourceConfig) -> SourceConnector:
    return SOURCE_CONNECTORS[cfg.type](cfg)  # type: ignore[arg-type]


def make_target_connector(cfg: TargetConfig) -> TargetConnector:
    return TARGET_CONNECTORS[cfg.type](cfg)  # type: ignore[arg-type]
