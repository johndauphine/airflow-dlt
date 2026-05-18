"""Single parameterized DAG that runs any pipeline defined under
``config/pipelines/*.yaml``.

Trigger the DAG with a config name in the DAG run params, e.g.::

    {"config_name": "stackoverflow"}

The DAG resolves ``{CONFIG_DIR}/<config_name>.yaml`` and runs the dlt
pipeline it describes. Scheduling, retries, and concurrency are properties
of *this DAG*, not of individual YAMLs — one knob per concern, one place to
change it.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from airflow_dlt.config import load_config
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
        available = sorted(p.stem for p in CONFIG_DIR.glob("*.yaml")) if CONFIG_DIR.is_dir() else []
        raise FileNotFoundError(
            f"config {config_name!r} not found at {path}. available: {available}"
        )
    return path


_DLT_RUNTIME_ENV = {
    "data_writer_file_max_items": "DATA_WRITER__FILE_MAX_ITEMS",
    "normalize_file_max_items": "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS",
    "normalize_file_max_bytes": "NORMALIZE__DATA_WRITER__FILE_MAX_BYTES",
    "normalize_workers": "NORMALIZE__WORKERS",
    "load_workers": "LOAD__WORKERS",
}


def _apply_dlt_runtime_env(runtime) -> None:
    """Set dlt runtime env from YAML and clear stale per-task overrides."""
    for attr, env_name in _DLT_RUNTIME_ENV.items():
        value = getattr(runtime, attr)
        if value is None:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = str(value)


@dag(
    dag_id="dlt_pipeline",
    description="Run a dlt pipeline defined by a YAML config; pick the YAML via the config_name DAG run param.",
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
    tags=["dlt"],
)
def dlt_pipeline_dag():
    @task
    def run() -> dict:
        # Lazy import: dlt pulls pyarrow at module load; keeping it out of the
        # DAG file's top-level imports avoids a numpy/pyarrow Cython init-order
        # bug under DagBag's parse subprocess (see the secrets_client rename
        # commit for context).
        from airflow.sdk import get_current_context

        from airflow_dlt.dlt_pipeline import build_pipeline

        # Fetch DAG run params explicitly rather than relying on Airflow's
        # @task auto-injection of `params: dict` — both work, but the
        # explicit form is unambiguous to humans and to static analyzers.
        params = get_current_context()["params"]
        config_name = params["config_name"]
        config_path = _resolve_config_path(config_name)
        log.info("Loading pipeline config from %s", config_path)

        cfg = load_config(config_path)
        _apply_dlt_runtime_env(cfg.dlt)
        secrets = MockDelineaClient(SECRETS_FILE) if SECRETS_FILE.is_file() else None
        # SQLite endpoints don't need secrets; allow running without a file
        # when both source and target are auth-free. build_pipeline will only
        # call secrets.get() when a connector has a secret_id.
        if secrets is None:
            from airflow_dlt.secrets_client import SecretsClient

            class _Empty(SecretsClient):
                def get(self, sid):
                    raise KeyError(f"no secrets file at {SECRETS_FILE}; needed for {sid}")

            secrets = _Empty()

        pipeline, source = build_pipeline(cfg, secrets)
        log.info(
            "Running dlt pipeline %s → dataset %s (%d tables)",
            pipeline.pipeline_name,
            pipeline.dataset_name,
            len(cfg.tables.include),
        )
        run_kwargs = {}
        if cfg.dlt.loader_file_format is not None:
            run_kwargs["loader_file_format"] = cfg.dlt.loader_file_format
        load_info = pipeline.run(source, **run_kwargs)
        log.info("Load complete: %s", load_info)
        return {
            "pipeline": pipeline.pipeline_name,
            "dataset": pipeline.dataset_name,
            "loads_ids": list(load_info.loads_ids),
        }

    run()


dlt_pipeline_dag()
