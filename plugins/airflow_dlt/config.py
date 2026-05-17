"""Pydantic models + loader for pipeline YAML configs.

Sources and targets are discriminated unions on ``type``. Adding a new
endpoint type means: add a Cfg model here, register it in the union, and add
a matching Connector in ``connectors.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Literal, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PipelineMeta(_Model):
    """Pipeline identity. Scheduling/retry/concurrency knobs deliberately live
    on the Airflow DAG itself, not in YAML — one DAG (`dlt_pipeline`) runs
    every config, so there's exactly one place for those operational knobs.
    The YAML's only job here is to name the pipeline (becomes dlt's
    ``pipeline_name`` state key and shows up in logs).
    """
    name: str

    @field_validator("name")
    @classmethod
    def _name_is_identifier(cls, v: str) -> str:
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"pipeline.name must be alphanumeric/underscore; got {v!r}"
            )
        return v


# ---------------------------------------------------------------------------
# Source configs (discriminated union on `type`)
# ---------------------------------------------------------------------------

class MssqlSourceCfg(_Model):
    type: Literal["mssql"]
    secret_id: str
    host: str
    port: int = 1433
    database: str
    schema_: str = Field(alias="schema")
    driver: str = "ODBC Driver 18 for SQL Server"
    options: dict[str, str] = Field(default_factory=dict)


class PostgresSourceCfg(_Model):
    type: Literal["postgres"]
    secret_id: str
    host: str
    port: int = 5432
    database: str
    schema_: str = Field(alias="schema")
    sslmode: str | None = None
    options: dict[str, str] = Field(default_factory=dict)


class SqliteSourceCfg(_Model):
    """SQLite source — file-based, no host/port/credentials.

    Path can be relative or absolute, or ``:memory:`` for an in-memory DB
    (only useful for tests where source and target share the same process).
    """
    type: Literal["sqlite"]
    path: str
    secret_id: str | None = None   # SQLite has no auth; field exists for shape parity


SourceConfig = Annotated[
    Union[MssqlSourceCfg, PostgresSourceCfg, SqliteSourceCfg],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Target configs (discriminated union on `type`)
# ---------------------------------------------------------------------------

class PostgresTargetCfg(_Model):
    type: Literal["postgres"]
    secret_id: str
    host: str
    port: int = 5432
    database: str
    schema_alias: str
    sslmode: str | None = None


class SqliteTargetCfg(_Model):
    """SQLite target — primarily for CI/tests without a database server."""
    type: Literal["sqlite"]
    path: str
    schema_alias: str
    secret_id: str | None = None   # field exists for shape parity; SQLite has no auth


TargetConfig = Annotated[
    Union[PostgresTargetCfg, SqliteTargetCfg],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Per-table + load config (unchanged)
# ---------------------------------------------------------------------------

WriteDisposition = Literal["replace", "append", "merge"] | dict[str, Any]


class IncrementalSpec(_Model):
    cursor_path: str
    initial_value: str | int | float | None = None
    range_start: Literal["open", "closed"] | None = None
    row_order: Literal["asc", "desc"] | None = None


class TableOverride(_Model):
    write_disposition: WriteDisposition | None = None
    primary_key: str | list[str] | None = None
    incremental: IncrementalSpec | None = None


class TablesConfig(_Model):
    include: list[str]
    overrides: dict[str, TableOverride] = Field(default_factory=dict)

    @field_validator("include")
    @classmethod
    def _include_non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("tables.include must contain at least one table")
        return v


class LoadConfig(_Model):
    write_disposition: WriteDisposition = "replace"
    chunk_size: int = 100_000


class PipelineConfig(_Model):
    pipeline: PipelineMeta
    source: SourceConfig
    target: TargetConfig
    tables: TablesConfig
    load: LoadConfig = Field(default_factory=LoadConfig)


def load_config(path: Path) -> PipelineConfig:
    """Read a YAML pipeline file and return a validated PipelineConfig."""
    raw: Any = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    return PipelineConfig.model_validate(raw)
