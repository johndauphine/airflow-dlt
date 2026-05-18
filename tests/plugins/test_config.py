from pathlib import Path

import pytest
from pydantic import ValidationError

from airflow_dlt.config import (
    MssqlSourceCfg,
    PostgresSourceCfg,
    PostgresTargetCfg,
    SqliteSourceCfg,
    SqliteTargetCfg,
    load_config,
)


MSSQL_TO_POSTGRES_YAML = """
pipeline:
  name: stackoverflow_mssql_to_postgres

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


def test_scheduling_fields_rejected(tmp_path):
    """schedule/retries/etc. used to be in PipelineMeta but were inert under
    the single-DAG model. Now strictly rejected — those knobs belong on the
    DAG, not the YAML."""
    bad = MSSQL_TO_POSTGRES_YAML.replace(
        "  name: stackoverflow_mssql_to_postgres\n",
        "  name: stackoverflow_mssql_to_postgres\n  schedule: '@daily'\n",
    )
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, bad))


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


def test_load_sqlite_to_sqlite_config(tmp_path):
    body = """
pipeline:
  name: sqlite_only
source:
  type: sqlite
  path: /tmp/source.db
target:
  type: sqlite
  path: /tmp/target.db
  schema_alias: ci
tables:
  include: [users]
"""
    cfg = load_config(_write(tmp_path, body))
    assert isinstance(cfg.source, SqliteSourceCfg)
    assert isinstance(cfg.target, SqliteTargetCfg)
    assert cfg.source.path == "/tmp/source.db"
    assert cfg.source.secret_id is None
    assert cfg.target.path == "/tmp/target.db"
    assert cfg.target.schema_alias == "ci"


def test_sqlite_source_rejects_postgres_specific_keys(tmp_path):
    """`host` only belongs on postgres/mssql sources; forbidden on sqlite."""
    body = """
pipeline:
  name: p1
source:
  type: sqlite
  path: /tmp/x.db
  host: somewhere
target:
  type: sqlite
  path: /tmp/y.db
  schema_alias: a
tables:
  include: [t]
"""
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, body))


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
    assert cfg.source.port == 1433
    assert cfg.target.port == 5432
    assert cfg.load.write_disposition == "replace"
    assert cfg.load.chunk_size == 100_000


@pytest.mark.parametrize(
    ("config_name", "table_name", "loader_file_format", "schema_alias"),
    [
        ("stackoverflow2013_votes_csv_copy_bench", "Votes", "csv", "csvcopy"),
        ("stackoverflow2013_votes_parquet_adbc_bench", "Votes", "parquet", "adbc"),
        ("stackoverflow2013_posts_csv_copy_bench", "Posts", "csv", "csvposts"),
        ("stackoverflow2013_posts_parquet_adbc_bench", "Posts", "parquet", "adbcposts"),
    ],
)
def test_loader_format_benchmark_configs_parse(
    config_name, table_name, loader_file_format, schema_alias
):
    cfg = load_config(
        Path(__file__).parents[2] / "config" / "pipelines" / f"{config_name}.yaml"
    )

    assert cfg.pipeline.name == config_name
    assert cfg.tables.include == [table_name]
    assert cfg.target.schema_alias == schema_alias
    assert cfg.dlt.sql_backend == "pyarrow"
    assert cfg.dlt.loader_file_format == loader_file_format
    assert cfg.dlt.load_workers == 5


def test_full_stackoverflow2013_benchmark_uses_parquet_adbc_loader():
    cfg = load_config(
        Path(__file__).parents[2] / "config" / "pipelines" / "stackoverflow2013_bench.yaml"
    )

    assert cfg.tables.include == [
        "Badges",
        "Comments",
        "LinkTypes",
        "PostLinks",
        "Posts",
        "PostTypes",
        "Users",
        "Votes",
        "VoteTypes",
    ]
    assert cfg.dlt.sql_backend == "pyarrow"
    assert cfg.dlt.loader_file_format == "parquet"
    assert cfg.dlt.load_workers == 5


@pytest.mark.parametrize(
    "config_name",
    [
        "stackoverflow2013_incremental_existing_bench",
        "example_stackoverflow2013_incremental",
    ],
)
def test_stackoverflow2013_incremental_configs_use_all_table_upserts(config_name):
    cfg = load_config(
        Path(__file__).parents[2]
        / "config"
        / "pipelines"
        / f"{config_name}.yaml"
    )

    assert cfg.tables.include == [
        "Badges",
        "Comments",
        "LinkTypes",
        "PostLinks",
        "Posts",
        "PostTypes",
        "Users",
        "Votes",
        "VoteTypes",
    ]
    assert set(cfg.tables.overrides) == set(cfg.tables.include)
    assert cfg.dlt.sql_backend == "pyarrow"
    assert cfg.dlt.loader_file_format == "parquet"

    expected_cursors = {
        "Badges": "Date",
        "Comments": "CreationDate",
        "LinkTypes": "Id",
        "PostLinks": "CreationDate",
        "Posts": "LastActivityDate",
        "PostTypes": "Id",
        "Users": "LastAccessDate",
        "Votes": "Id",
        "VoteTypes": "Id",
    }
    for table_name, cursor_path in expected_cursors.items():
        override = cfg.tables.overrides[table_name]
        assert override.primary_key == "Id"
        assert override.write_disposition == {
            "disposition": "merge",
            "strategy": "upsert",
        }
        assert override.incremental is not None
        assert override.incremental.cursor_path == cursor_path
        assert override.incremental.initial_value is None
        assert override.incremental.range_start == "open"
        assert override.incremental.row_order == "asc"
