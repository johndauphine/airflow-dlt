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
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task

from airflow_dlt.config import PipelineConfig, load_config
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


def _sanitize_pipeline_suffix(table_name: str) -> str:
    """Return a pipeline-name-safe suffix derived from a source table name."""
    suffix = re.sub(r"[^0-9A-Za-z]+", "_", table_name).strip("_").lower()
    if not suffix:
        raise ValueError(f"table name {table_name!r} does not produce a valid pipeline suffix")
    return suffix


def _table_specs(cfg: PipelineConfig, config_name: str) -> list[dict[str, str]]:
    """Build JSON-serializable mapped task specs from one validated config."""
    tables = cfg.tables.include
    if len(tables) == 1:
        return [{
            "config_name": config_name,
            "table_name": tables[0],
            "pipeline_name": cfg.pipeline.name,
        }]

    specs: list[dict[str, str]] = []
    suffix_to_table: dict[str, str] = {}
    for table_name in tables:
        suffix = _sanitize_pipeline_suffix(table_name)
        if suffix in suffix_to_table:
            raise ValueError(
                "tables produce duplicate pipeline suffix "
                f"{suffix!r}: {suffix_to_table[suffix]!r}, {table_name!r}"
            )
        suffix_to_table[suffix] = table_name
        specs.append({
            "config_name": config_name,
            "table_name": table_name,
            "pipeline_name": f"{cfg.pipeline.name}_{suffix}",
        })
    return specs


def _secrets_client():
    """Return the configured secrets client, or an auth-free placeholder."""
    if SECRETS_FILE.is_file():
        return MockDelineaClient(SECRETS_FILE)

    from airflow_dlt.secrets_client import SecretsClient

    class _Empty(SecretsClient):
        def get(self, sid):
            raise KeyError(f"no secrets file at {SECRETS_FILE}; needed for {sid}")

    return _Empty()


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
    def read_config() -> list[dict[str, str]]:
        from airflow.sdk import get_current_context

        params = get_current_context()["params"]
        config_name = params["config_name"]
        config_path = _resolve_config_path(config_name)
        log.info("Loading pipeline config from %s", config_path)
        cfg = load_config(config_path)
        specs = _table_specs(cfg, config_name)
        log.info("Config %s will map to %d table task(s)", config_name, len(specs))
        return specs

    @task(pool="dlt_table_loads")
    def run_table(table_spec: dict[str, str]) -> dict[str, Any]:
        # Lazy import: dlt pulls pyarrow at module load; keeping it out of the
        # DAG file's top-level imports avoids a numpy/pyarrow Cython init-order
        # bug under DagBag's parse subprocess (see the secrets_client rename
        # commit for context).
        from airflow_dlt.dlt_pipeline import build_pipeline

        config_name = table_spec["config_name"]
        table_name = table_spec["table_name"]
        pipeline_name = table_spec["pipeline_name"]
        config_path = _resolve_config_path(config_name)
        log.info("Loading pipeline config from %s", config_path)

        cfg = load_config(config_path)
        _apply_dlt_runtime_env(cfg.dlt)
        secrets = _secrets_client()

        pipeline, source = build_pipeline(
            cfg,
            secrets,
            table_names=[table_name],
            pipeline_name=pipeline_name,
        )
        log.info(
            "Running dlt pipeline %s → dataset %s (%s)",
            pipeline.pipeline_name,
            pipeline.dataset_name,
            table_name,
        )
        run_kwargs = {}
        if cfg.dlt.loader_file_format is not None:
            run_kwargs["loader_file_format"] = cfg.dlt.loader_file_format
        load_info = pipeline.run(source, **run_kwargs)
        log.info("Load complete: %s", load_info)
        return {
            "base_pipeline": cfg.pipeline.name,
            "pipeline": pipeline.pipeline_name,
            "dataset": pipeline.dataset_name,
            "table": table_name,
            "loads_ids": list(load_info.loads_ids),
        }

    run_table.expand(table_spec=read_config())


dlt_pipeline_dag()
