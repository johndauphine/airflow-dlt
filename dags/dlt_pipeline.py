"""Single parameterized DAG that runs a dlt pipeline defined by a YAML file.

Trigger the DAG with a config name in DAG run params, e.g.::

    {"config_name": "stackoverflow"}

The DAG resolves ``/opt/airflow/config/pipelines/<config_name>.yaml`` and runs
the dlt pipeline it describes. All connection details (source MSSQL + target
Postgres) come from the YAML; credentials come from the (mock) Delinea client.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from airflow_dlt.config import load_config
from airflow_dlt.dlt_pipeline import build_pipeline
from airflow_dlt.secrets_mock import MockDelineaClient

log = logging.getLogger(__name__)

CONFIG_DIR = Path(os.environ.get("PIPELINE_CONFIG_DIR", "/opt/airflow/config/pipelines"))
SECRETS_FILE = Path(os.environ.get("PIPELINE_SECRETS_FILE", "/opt/airflow/config/secrets.yaml"))


def _resolve_config_path(config_name: str) -> Path:
    """Map a config_name DAG param to a YAML file path, with traversal guard."""
    if not config_name or "/" in config_name or ".." in config_name:
        raise ValueError(f"invalid config_name: {config_name!r}")
    path = CONFIG_DIR / f"{config_name}.yaml"
    if not path.is_file():
        available = sorted(p.stem for p in CONFIG_DIR.glob("*.yaml"))
        raise FileNotFoundError(
            f"config {config_name!r} not found at {path}. available: {available}"
        )
    return path


@dag(
    dag_id="dlt_pipeline",
    description="Run a dlt MSSQL→Postgres pipeline defined by a YAML config file.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    params={"config_name": "stackoverflow"},
    tags=["dlt", "mssql", "postgres"],
)
def dlt_pipeline_dag():
    @task
    def run(params: dict) -> dict:
        config_name = params["config_name"]
        config_path = _resolve_config_path(config_name)
        log.info("Loading pipeline config from %s", config_path)

        cfg = load_config(config_path)
        secrets = MockDelineaClient(SECRETS_FILE)
        pipeline, source = build_pipeline(cfg, secrets)

        log.info(
            "Running dlt pipeline %s → dataset %s (%d tables)",
            pipeline.pipeline_name,
            pipeline.dataset_name,
            len(cfg.tables.include),
        )
        load_info = pipeline.run(source)
        log.info("Load complete: %s", load_info)
        return {
            "pipeline": pipeline.pipeline_name,
            "dataset": pipeline.dataset_name,
            "loads_ids": list(load_info.loads_ids),
        }

    run()


dlt_pipeline_dag()
