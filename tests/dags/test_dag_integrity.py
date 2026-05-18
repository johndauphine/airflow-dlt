"""DAG-bag integrity checks for the single parameterized DAG."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

REPO = Path(__file__).resolve().parents[2]
os.environ.setdefault("PIPELINE_CONFIG_DIR", str(REPO / "config" / "pipelines"))
os.environ.setdefault("PIPELINE_SECRETS_FILE", str(REPO / "config" / "secrets.yaml.example"))
sys.path.insert(0, str(REPO / "plugins"))

from airflow.models import DagBag  # noqa: E402 - must follow sys.path setup above


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(REPO / "dags"), include_examples=False)


def test_dags_parse_without_errors(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, f"DAG parse errors: {dagbag.import_errors}"


def test_dlt_pipeline_dag_exists(dagbag: DagBag) -> None:
    """The single parameterized DAG must always be present. Additional
    user-defined scheduled DAGs (see dags/example_scheduled.py) may
    coexist alongside it."""
    assert "dlt_pipeline" in dagbag.dag_ids, (
        f"dlt_pipeline missing from {list(dagbag.dag_ids)}"
    )


def test_dlt_pipeline_accepts_config_name_param(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert "config_name" in dag.params


def test_dlt_pipeline_maps_table_tasks(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert "read_config" in dag.task_ids
    assert "run_table" in dag.task_ids
    assert dag.get_task("run_table").pool == "dlt_table_loads"


def test_all_dags_have_tags(dagbag: DagBag) -> None:
    """Every DAG — built-in or user-added — must declare at least one tag."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} is missing tags"


def test_all_dags_have_retries(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        retries = (dag.default_args or {}).get("retries", 0)
        assert retries >= 1, f"DAG {dag_id} has retries={retries}"


def test_template_scheduled_dag_is_paused_by_default(dagbag: DagBag) -> None:
    """The example_scheduled.py template must NOT auto-fire on clone — it
    points at the shipped config and would trigger loads against whoever's
    secrets.yaml is in the container."""
    if "example_dlt_stackoverflow_hourly" not in dagbag.dag_ids:
        return  # the file may have been removed; that's fine
    dag = dagbag.dags["example_dlt_stackoverflow_hourly"]
    assert dag.is_paused_upon_creation is True, (
        "template scheduled DAG must set is_paused_upon_creation=True"
    )


# ---------------------------------------------------------------------------
# Resolver — directly test _resolve_config_path against synthetic dirs.
# ---------------------------------------------------------------------------

def _load_resolver():
    sys.path.insert(0, str(REPO / "dags"))
    import importlib

    mod = importlib.import_module("dlt_pipeline")
    return mod._resolve_config_path, mod


def test_resolver_finds_existing_config():
    resolver, _ = _load_resolver()
    # The example stackoverflow.yaml exists in the repo's config dir.
    p = resolver("stackoverflow")
    assert p.name == "stackoverflow.yaml"


def test_resolver_rejects_traversal():
    resolver, _ = _load_resolver()
    with pytest.raises(ValueError):
        resolver("../etc/passwd")
    with pytest.raises(ValueError):
        resolver("foo/bar")
    with pytest.raises(ValueError):
        resolver("")


def test_resolver_reports_available_on_miss(tmp_path, monkeypatch):
    """When the config_name doesn't exist, the error message lists what's
    actually available so the operator can self-correct."""
    (tmp_path / "alpha.yaml").write_text("")
    (tmp_path / "bravo.yaml").write_text("")

    _, mod = _load_resolver()
    monkeypatch.setattr(mod, "CONFIG_DIR", tmp_path)

    with pytest.raises(FileNotFoundError) as exc:
        mod._resolve_config_path("missing")
    msg = str(exc.value)
    assert "alpha" in msg and "bravo" in msg


def test_table_specs_suffix_multi_table_pipeline_names():
    _, mod = _load_resolver()
    from airflow_dlt.config import PipelineConfig

    cfg = PipelineConfig.model_validate({
        "pipeline": {"name": "base_pipe"},
        "source": {
            "type": "sqlite",
            "path": "/tmp/source.db",
        },
        "target": {
            "type": "sqlite",
            "path": "/tmp/target.db",
            "schema_alias": "dev",
        },
        "tables": {"include": ["Users", "Post-Links"]},
    })

    assert mod._table_specs(cfg, "cfg") == [
        {
            "config_name": "cfg",
            "table_name": "Users",
            "pipeline_name": "base_pipe_users",
        },
        {
            "config_name": "cfg",
            "table_name": "Post-Links",
            "pipeline_name": "base_pipe_post_links",
        },
    ]


def test_table_specs_preserve_single_table_pipeline_name():
    _, mod = _load_resolver()
    from airflow_dlt.config import PipelineConfig

    cfg = PipelineConfig.model_validate({
        "pipeline": {"name": "single_pipe"},
        "source": {
            "type": "sqlite",
            "path": "/tmp/source.db",
        },
        "target": {
            "type": "sqlite",
            "path": "/tmp/target.db",
            "schema_alias": "dev",
        },
        "tables": {"include": ["Users"]},
    })

    assert mod._table_specs(cfg, "cfg") == [
        {
            "config_name": "cfg",
            "table_name": "Users",
            "pipeline_name": "single_pipe",
        }
    ]


def test_table_specs_reject_duplicate_sanitized_suffixes():
    _, mod = _load_resolver()
    from airflow_dlt.config import PipelineConfig

    cfg = PipelineConfig.model_validate({
        "pipeline": {"name": "base_pipe"},
        "source": {
            "type": "sqlite",
            "path": "/tmp/source.db",
        },
        "target": {
            "type": "sqlite",
            "path": "/tmp/target.db",
            "schema_alias": "dev",
        },
        "tables": {"include": ["Post-Links", "Post Links"]},
    })

    with pytest.raises(ValueError, match="duplicate pipeline suffix"):
        mod._table_specs(cfg, "cfg")
