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

from airflow.models import DagBag


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
