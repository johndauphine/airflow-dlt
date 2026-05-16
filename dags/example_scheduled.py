"""Template: a scheduled DAG that runs one specific YAML pipeline.

The ``dlt_pipeline`` DAG handles ad-hoc / manual runs — operators trigger it
with ``{"config_name": "..."}`` to pick the YAML at trigger time. For
production *scheduled* loads, copy this file, rename the dag_id, point
``CONFIG_PATH`` at the YAML you want, and set the schedule you need.

Two DAGs can target the same YAML on different schedules; a YAML can also
be both manually triggerable (via ``dlt_pipeline``) and scheduled (via a
file like this one). The YAML stays the source of truth for *what* to do;
the DAG controls *when*.

This template is paused-on-creation so cloning the repo doesn't start
firing hourly loads against whoever's `config/secrets.yaml` points at.
Unpause it from the Airflow UI (or copy + adapt to your own dag_id) to
actually use it.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# Point at one specific YAML. Change me when you copy this template.
CONFIG_PATH = Path("/opt/airflow/config/pipelines/stackoverflow.yaml")
SECRETS_FILE = Path("/opt/airflow/config/secrets.yaml")


@dag(
    dag_id="example_dlt_stackoverflow_hourly",
    description=f"Scheduled dlt load of {CONFIG_PATH.name} — template; edit and unpause to use.",
    schedule="@hourly",                       # any cron, timedelta, or Airflow Asset
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,             # template — don't auto-fire
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dlt", "scheduled", "template"],
)
def example_scheduled_dag():
    @task
    def run() -> dict:
        # Same lazy-import pattern as dlt_pipeline.py — avoids the
        # pyarrow/numpy DagBag init bug under Airflow's parse subprocess.
        from airflow_dlt.config import load_config
        from airflow_dlt.dlt_pipeline import build_pipeline
        from airflow_dlt.secrets_mock import MockDelineaClient

        cfg = load_config(CONFIG_PATH)
        secrets = MockDelineaClient(SECRETS_FILE)
        pipeline, source = build_pipeline(cfg, secrets)
        load_info = pipeline.run(source)
        return {
            "pipeline": pipeline.pipeline_name,
            "dataset": pipeline.dataset_name,
            "loads_ids": list(load_info.loads_ids),
        }

    run()


example_scheduled_dag()
