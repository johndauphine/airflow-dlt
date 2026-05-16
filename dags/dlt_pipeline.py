"""DAG factory: one Airflow DAG per pipeline YAML under config/pipelines/.

At parse time we walk ``CONFIG_DIR`` and register one DAG per ``*.yaml``
file. The DAG's ``dag_id``, ``schedule``, ``retries``, ``retry_delay``, and
``max_active_runs`` all come from the YAML — editing those YAML fields
changes Airflow behavior on the next DAG reparse.

YAML files that fail to parse register a *broken* DAG whose only task fails
loudly with the parse error. This is preferred over swallowing the error
(silent missing DAGs are operationally invisible).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from airflow_dlt.config import PipelineConfig, load_config
from airflow_dlt.dlt_pipeline import build_pipeline
from airflow_dlt.secrets_mock import MockDelineaClient

log = logging.getLogger(__name__)

CONFIG_DIR = Path(os.environ.get("PIPELINE_CONFIG_DIR", "/opt/airflow/config/pipelines"))
SECRETS_FILE = Path(os.environ.get("PIPELINE_SECRETS_FILE", "/opt/airflow/config/secrets.yaml"))


def _make_dag(yaml_path: Path, cfg: PipelineConfig):
    @dag(
        dag_id=f"dlt_{cfg.pipeline.name}",
        description=f"dlt MSSQL→Postgres pipeline defined by {yaml_path.name}",
        start_date=datetime(2026, 1, 1),
        schedule=cfg.pipeline.schedule,
        catchup=False,
        max_active_runs=cfg.pipeline.max_active_runs,
        default_args={
            "owner": "data-team",
            "retries": cfg.pipeline.retries,
            "retry_delay": timedelta(seconds=cfg.pipeline.retry_delay_seconds),
        },
        tags=["dlt", cfg.source.type, cfg.target.type],
    )
    def _pipeline_dag():
        @task
        def run() -> dict:
            # Re-read config at task runtime so YAML edits take effect on the
            # next DAG run without requiring a scheduler reparse.
            runtime_cfg = load_config(yaml_path)
            secrets = MockDelineaClient(SECRETS_FILE)
            pipeline, source = build_pipeline(runtime_cfg, secrets)

            log.info(
                "Running dlt pipeline %s → dataset %s (%d tables)",
                pipeline.pipeline_name,
                pipeline.dataset_name,
                len(runtime_cfg.tables.include),
            )
            load_info = pipeline.run(source)
            log.info("Load complete: %s", load_info)
            return {
                "pipeline": pipeline.pipeline_name,
                "dataset": pipeline.dataset_name,
                "loads_ids": list(load_info.loads_ids),
            }

        run()

    return _pipeline_dag()


def _make_broken_dag(yaml_path: Path, error: Exception):
    """Register a DAG whose only task re-raises the YAML parse error.

    Better than silently skipping the file: operators see a failing DAG in
    Airflow rather than a missing one.
    """
    dag_id = f"dlt_broken__{yaml_path.stem}"

    @dag(
        dag_id=dag_id,
        description=f"BROKEN pipeline config {yaml_path.name}: {error!r}",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"owner": "data-team", "retries": 0},
        tags=["dlt", "broken-config"],
    )
    def _broken_dag():
        @task
        def fail() -> None:
            raise RuntimeError(
                f"Pipeline config {yaml_path} failed to parse: {error!r}"
            )

        fail()

    return _broken_dag()


def _register_all() -> None:
    if not CONFIG_DIR.is_dir():
        log.warning("CONFIG_DIR %s does not exist; no pipeline DAGs registered", CONFIG_DIR)
        return
    for yaml_path in sorted(CONFIG_DIR.glob("*.yaml")):
        try:
            cfg = load_config(yaml_path)
        except Exception as exc:  # noqa: BLE001 - we want any parse error
            log.error("Failed to parse %s: %r", yaml_path, exc)
            broken = _make_broken_dag(yaml_path, exc)
            globals()[broken.dag_id] = broken
            continue
        d = _make_dag(yaml_path, cfg)
        globals()[d.dag_id] = d


_register_all()
