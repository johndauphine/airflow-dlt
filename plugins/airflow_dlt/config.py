"""Pydantic models + loader for pipeline YAML configs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PipelineMeta(_Model):
    name: str
    schedule: str | None = None
    max_active_runs: int = 1
    retries: int = 3
    retry_delay_seconds: int = 30

    @field_validator("name")
    @classmethod
    def _name_is_identifier(cls, v: str) -> str:
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"pipeline.name must be alphanumeric/underscore; got {v!r}"
            )
        return v


class SourceConfig(_Model):
    type: Literal["mssql"]
    secret_id: str
    host: str
    port: int = 1433
    database: str
    schema_: str = Field(alias="schema")
    driver: str = "ODBC Driver 18 for SQL Server"
    options: dict[str, str] = Field(default_factory=dict)


class TargetConfig(_Model):
    type: Literal["postgres"]
    secret_id: str
    host: str
    port: int = 5432
    database: str
    schema_alias: str


class IncrementalSpec(_Model):
    cursor_path: str
    initial_value: str | int | float | None = None


class TableOverride(_Model):
    write_disposition: Literal["replace", "append", "merge"] | None = None
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
    write_disposition: Literal["replace", "append", "merge"] = "replace"
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
