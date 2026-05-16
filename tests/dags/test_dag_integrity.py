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


def test_exactly_one_dag(dagbag: DagBag) -> None:
    """Single parameterized DAG model: there should be exactly one DAG, not
    one per YAML."""
    assert list(dagbag.dag_ids) == ["dlt_pipeline"], (
        f"expected just dlt_pipeline; got {list(dagbag.dag_ids)}"
    )


def test_dag_accepts_config_name_param(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert "config_name" in dag.params


def test_dag_has_tags(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert dag.tags, "dlt_pipeline DAG must have tags"


def test_dag_has_retries(dagbag: DagBag) -> None:
    dag = dagbag.dags["dlt_pipeline"]
    assert dag.default_args.get("retries", 0) >= 1


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
