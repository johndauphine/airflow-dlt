"""DAG-bag integrity checks.

The DAG factory in dags/dlt_pipeline.py walks a config dir at parse time; we
point it at the repo's actual config/pipelines so the example pipeline must
parse cleanly.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
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


def test_at_least_one_pipeline_dag_registered(dagbag: DagBag) -> None:
    pipeline_dags = [d for d in dagbag.dag_ids if d.startswith("dlt_")]
    assert pipeline_dags, "no dlt_* pipeline DAGs registered from config/pipelines"


def test_stackoverflow_example_dag_present(dagbag: DagBag) -> None:
    assert "dlt_stackoverflow_mssql_to_postgres" in dagbag.dag_ids


def test_no_broken_config_dags(dagbag: DagBag) -> None:
    broken = [d for d in dagbag.dag_ids if d.startswith("dlt_broken__")]
    assert not broken, f"broken-config DAGs registered (YAML parse failed): {broken}"


def test_all_dags_have_tags(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} is missing tags"


def test_pipeline_dag_picks_up_yaml_retry_settings(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_stackoverflow_mssql_to_postgres"]
    # Example YAML sets retries=3, retry_delay_seconds=30.
    assert dag.default_args["retries"] == 3
    assert dag.default_args["retry_delay"] == timedelta(seconds=30)


# ---------------------------------------------------------------------------
# Factory robustness — these import the module's _register_all directly so we
# can drive it against synthetic config dirs without restarting Airflow.
# ---------------------------------------------------------------------------

_BASE_YAML = """
pipeline:
  name: {name}
  {schedule_line}
source:
  type: mssql
  secret_id: src
  host: h
  database: d
  schema: dbo
target:
  type: postgres
  secret_id: tgt
  host: h
  database: d
  schema_alias: a
tables:
  include: [T1]
"""


def _yaml(name: str, schedule: str | None = None) -> str:
    schedule_line = f'schedule: "{schedule}"' if schedule else ""
    return _BASE_YAML.format(name=name, schedule_line=schedule_line)


def _load_register_all():
    sys.path.insert(0, str(REPO / "dags"))
    import importlib

    mod = importlib.import_module("dlt_pipeline")
    return mod._register_all


def test_unparseable_yaml_yields_broken_dag(tmp_path):
    (tmp_path / "good.yaml").write_text(_yaml("good_one"))
    (tmp_path / "bad.yaml").write_text("not: [valid: yaml")  # bad YAML
    register_all = _load_register_all()

    registered = register_all(tmp_path)
    assert "dlt_good_one" in registered
    assert "dlt_broken__bad" in registered


def test_invalid_schedule_yields_broken_dag(tmp_path):
    (tmp_path / "good.yaml").write_text(_yaml("good_two"))
    (tmp_path / "bad_cron.yaml").write_text(
        _yaml("bad_cron_pipeline", schedule="this is not a cron")
    )
    register_all = _load_register_all()

    registered = register_all(tmp_path)
    assert "dlt_good_two" in registered
    # The bad-cron file MUST register as broken (not as a normal DAG that
    # would later blow up DagBag.validate() and hide every sibling DAG).
    assert "dlt_bad_cron_pipeline" not in registered
    assert "dlt_broken__bad_cron" in registered


def test_duplicate_pipeline_name_yields_broken_dag(tmp_path):
    (tmp_path / "a.yaml").write_text(_yaml("dup_name"))
    (tmp_path / "b.yaml").write_text(_yaml("dup_name"))
    register_all = _load_register_all()

    registered = register_all(tmp_path)
    # First (alphabetical) wins as the real DAG.
    assert "dlt_dup_name" in registered
    # Second one registers as broken so the conflict is visible.
    assert "dlt_broken__b" in registered


def test_factory_does_not_leak_dags_via_auto_register(tmp_path):
    """Invalid configs must not leak into DagContext.autoregistered_dags.

    Otherwise DagBag's auto-register sweep would pick up the invalid DAG
    and reintroduce the import error this factory exists to prevent.
    """
    from airflow.sdk.definitions._internal.contextmanager import DagContext

    (tmp_path / "good.yaml").write_text(_yaml("good_three"))
    (tmp_path / "bad_cron.yaml").write_text(
        _yaml("bad_cron_two", schedule="this is not a cron")
    )
    register_all = _load_register_all()

    DagContext.autoregistered_dags.clear()
    register_all(tmp_path)
    leaked = {d.dag_id for d, _mod in DagContext.autoregistered_dags}
    assert leaked == set(), f"DAGs leaked via auto-register: {leaked}"
