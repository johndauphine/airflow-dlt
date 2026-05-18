from __future__ import annotations

from types import SimpleNamespace

from airflow_dlt.config import PipelineConfig
from airflow_dlt.dlt_pipeline import build_pipeline


class _Secrets:
    def get(self, secret_id):
        return {"username": secret_id, "password": "pw"}


class _Resource:
    def __init__(self) -> None:
        self.hints: list[dict] = []

    def apply_hints(self, **kwargs) -> None:
        self.hints.append(kwargs)


class _Source:
    def __init__(self, table_names: list[str]) -> None:
        self.resources = {table_name: _Resource() for table_name in table_names}


class _SourceConnector:
    def sqlalchemy_url(self, creds):
        return f"source://{creds['username']}"

    def schema_name(self):
        return "dbo"

    def database_name(self):
        return "StackOverflow2013"


class _TargetConnector:
    def build_destination(self, creds):
        return f"target://{creds['username']}"


def _cfg() -> PipelineConfig:
    return PipelineConfig.model_validate({
        "pipeline": {"name": "base_pipe"},
        "source": {
            "type": "mssql",
            "secret_id": "source_secret",
            "host": "mssql",
            "database": "StackOverflow2013",
            "schema": "dbo",
        },
        "target": {
            "type": "postgres",
            "secret_id": "target_secret",
            "host": "pg",
            "database": "warehouse",
            "schema_alias": "dlt",
        },
        "tables": {
            "include": ["Users", "Posts"],
            "overrides": {
                "Users": {"primary_key": "Id"},
                "Posts": {"primary_key": "Id"},
            },
        },
        "load": {"write_disposition": "replace", "chunk_size": 1000},
    })


def _patch_builder(monkeypatch):
    import airflow_dlt.dlt_pipeline as mod

    calls = {}

    def fake_sql_database(**kwargs):
        calls["sql_database"] = kwargs
        return _Source(kwargs["table_names"])

    def fake_pipeline(**kwargs):
        calls["pipeline"] = kwargs
        return SimpleNamespace(
            pipeline_name=kwargs["pipeline_name"],
            dataset_name=kwargs["dataset_name"],
        )

    monkeypatch.setattr(mod, "make_source_connector", lambda cfg: _SourceConnector())
    monkeypatch.setattr(mod, "make_target_connector", lambda cfg: _TargetConnector())
    monkeypatch.setattr(mod, "sql_database", fake_sql_database)
    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline)
    return calls


def test_build_pipeline_defaults_to_configured_tables_and_pipeline_name(monkeypatch):
    calls = _patch_builder(monkeypatch)

    pipeline, source = build_pipeline(_cfg(), _Secrets())

    assert calls["sql_database"]["table_names"] == ["Users", "Posts"]
    assert calls["pipeline"]["pipeline_name"] == "base_pipe"
    assert pipeline.pipeline_name == "base_pipe"
    assert sorted(source.resources) == ["Posts", "Users"]
    assert source.resources["Users"].hints == [
        {"write_disposition": "replace"},
        {"primary_key": "Id"},
    ]


def test_build_pipeline_can_select_one_table_and_pipeline_name(monkeypatch):
    calls = _patch_builder(monkeypatch)

    pipeline, source = build_pipeline(
        _cfg(),
        _Secrets(),
        table_names=["Posts"],
        pipeline_name="base_pipe_posts",
    )

    assert calls["sql_database"]["table_names"] == ["Posts"]
    assert calls["pipeline"]["pipeline_name"] == "base_pipe_posts"
    assert pipeline.pipeline_name == "base_pipe_posts"
    assert sorted(source.resources) == ["Posts"]
    assert source.resources["Posts"].hints == [
        {"write_disposition": "replace"},
        {"primary_key": "Id"},
    ]


def test_build_pipeline_ignores_overrides_for_non_selected_tables(monkeypatch):
    _patch_builder(monkeypatch)

    _, source = build_pipeline(_cfg(), _Secrets(), table_names=["Posts"])

    assert "Users" not in source.resources
    assert source.resources["Posts"].hints == [
        {"write_disposition": "replace"},
        {"primary_key": "Id"},
    ]
