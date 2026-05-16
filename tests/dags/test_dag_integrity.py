"""DAG-bag integrity checks. These mirror the template repo's smoke tests."""

from __future__ import annotations

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)


def test_dags_parse_without_errors(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, (
        f"DAG parse errors: {dagbag.import_errors}"
    )


def test_dlt_pipeline_dag_present(dagbag: DagBag) -> None:
    assert "dlt_pipeline" in dagbag.dag_ids


def test_all_dags_have_tags(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} is missing tags"


def test_all_dags_have_retries(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        retries = (dag.default_args or {}).get("retries", 0)
        assert retries >= 1, f"DAG {dag_id} has retries={retries}"


def test_dlt_pipeline_has_config_name_param(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert "config_name" in dag.params, "dlt_pipeline must accept config_name param"
