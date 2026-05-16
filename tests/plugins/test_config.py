from pathlib import Path

import pytest
from pydantic import ValidationError

from airflow_dlt.config import (
    MssqlSourceCfg,
    PostgresSourceCfg,
    PostgresTargetCfg,
    load_config,
)


MSSQL_TO_POSTGRES_YAML = """
pipeline:
  name: stackoverflow_mssql_to_postgres
  schedule: null
  retries: 3
  retry_delay_seconds: 30

source:
  type: mssql
  secret_id: source_mssql_stackoverflow
  host: mssql-server
  port: 1433
  database: StackOverflow2010
  schema: dbo

target:
  type: postgres
  secret_id: target_postgres_main
  host: postgres-target
  port: 5432
  database: stackoverflow
  schema_alias: dev

tables:
  include:
    - Users
    - Posts
  overrides:
    Users:
      write_disposition: merge
      primary_key: Id
      incremental:
        cursor_path: ModifiedDate

load:
  write_disposition: replace
  chunk_size: 100000
"""


POSTGRES_TO_POSTGRES_YAML = """
pipeline:
  name: pg_replication
source:
  type: postgres
  secret_id: src_pg
  host: source-pg
  database: app
  schema: public
  sslmode: require
target:
  type: postgres
  secret_id: tgt_pg
  host: warehouse-pg
  database: warehouse
  schema_alias: prod
tables:
  include: [orders]
"""


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "pipeline.yaml"
    p.write_text(body)
    return p


def test_load_mssql_to_postgres_config(tmp_path):
    cfg = load_config(_write(tmp_path, MSSQL_TO_POSTGRES_YAML))
    assert cfg.pipeline.name == "stackoverflow_mssql_to_postgres"
    assert isinstance(cfg.source, MssqlSourceCfg)
    assert isinstance(cfg.target, PostgresTargetCfg)
    assert cfg.source.schema_ == "dbo"
    assert cfg.target.schema_alias == "dev"
    assert cfg.tables.overrides["Users"].incremental.cursor_path == "ModifiedDate"


def test_load_postgres_to_postgres_config(tmp_path):
    cfg = load_config(_write(tmp_path, POSTGRES_TO_POSTGRES_YAML))
    assert isinstance(cfg.source, PostgresSourceCfg)
    assert cfg.source.sslmode == "require"
    assert cfg.source.port == 5432  # default applied


def test_pipeline_name_must_be_identifier(tmp_path):
    bad = MSSQL_TO_POSTGRES_YAML.replace("stackoverflow_mssql_to_postgres", "has space")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_empty_include_rejected(tmp_path):
    bad = MSSQL_TO_POSTGRES_YAML.replace(
        "  include:\n    - Users\n    - Posts\n", "  include: []\n"
    )
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_unknown_source_type_rejected(tmp_path):
    bad = MSSQL_TO_POSTGRES_YAML.replace("type: mssql", "type: oracle")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_unknown_target_type_rejected(tmp_path):
    bad = MSSQL_TO_POSTGRES_YAML.replace(
        "  type: postgres\n  secret_id: target_postgres_main",
        "  type: snowflake\n  secret_id: target_postgres_main",
    )
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_extra_keys_rejected(tmp_path):
    bad = MSSQL_TO_POSTGRES_YAML + "extra_top_level: nope\n"
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_mssql_specific_keys_rejected_on_postgres_source(tmp_path):
    """`driver` only belongs on mssql; should be forbidden on a postgres source."""
    bad = """
pipeline:
  name: p1
source:
  type: postgres
  secret_id: s
  host: h
  database: d
  schema: public
  driver: "ODBC Driver 18 for SQL Server"
target:
  type: postgres
  secret_id: t
  host: h
  database: d
  schema_alias: a
tables:
  include: [T1]
"""
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


def test_defaults_applied_mssql(tmp_path):
    minimal = """
pipeline:
  name: p1
source:
  type: mssql
  secret_id: s
  host: h
  database: d
  schema: dbo
target:
  type: postgres
  secret_id: t
  host: h
  database: d
  schema_alias: a
tables:
  include: [T1]
"""
    cfg = load_config(_write(tmp_path, minimal))
    assert cfg.pipeline.retries == 3
    assert cfg.source.port == 1433
    assert cfg.target.port == 5432
    assert cfg.load.write_disposition == "replace"
    assert cfg.load.chunk_size == 100_000
