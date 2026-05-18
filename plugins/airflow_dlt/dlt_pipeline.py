"""Construct a dlt Pipeline + source from a validated YAML config.

Type-agnostic: the source and target connectors (``connectors.py``) own
everything specific to MSSQL / Postgres / etc. ``build_pipeline`` just
orchestrates: look up connectors, hand them credentials, hand the result to
dlt.
"""

from __future__ import annotations

from typing import Any

import dlt
from dlt.sources.sql_database import sql_database

from airflow_dlt.config import PipelineConfig, TableOverride
from airflow_dlt.connectors import make_source_connector, make_target_connector
from airflow_dlt.schema_naming import derive_dataset_name
from airflow_dlt.secrets_client import SecretsClient


def _apply_table_overrides(source: Any, overrides: dict[str, TableOverride]) -> None:
    """Apply per-table primary_key / write_disposition / incremental hints."""
    for table_name, override in overrides.items():
        if table_name not in source.resources:
            raise KeyError(
                f"override targets table {table_name!r} not present in source.resources "
                f"(known: {sorted(source.resources.keys())})"
            )
        resource = source.resources[table_name]
        hints: dict[str, Any] = {}
        if override.primary_key is not None:
            hints["primary_key"] = override.primary_key
        if override.write_disposition is not None:
            hints["write_disposition"] = override.write_disposition
        if override.incremental is not None:
            incremental_kwargs: dict[str, Any] = {
                "cursor_path": override.incremental.cursor_path,
                "initial_value": override.incremental.initial_value,
            }
            if override.incremental.range_start is not None:
                incremental_kwargs["range_start"] = override.incremental.range_start
            if override.incremental.row_order is not None:
                incremental_kwargs["row_order"] = override.incremental.row_order
            hints["incremental"] = dlt.sources.incremental(**incremental_kwargs)
        if hints:
            resource.apply_hints(**hints)


def _resolve(secrets: SecretsClient, secret_id: str | None) -> dict[str, str]:
    """Return credentials dict, or {} for endpoints with no auth (SQLite)."""
    return secrets.get(secret_id) if secret_id else {}


def build_pipeline(
    cfg: PipelineConfig,
    secrets: SecretsClient,
    *,
    table_names: list[str] | None = None,
    pipeline_name: str | None = None,
) -> tuple[Any, Any]:
    """Return a ``(pipeline, source)`` pair ready for ``pipeline.run(source)``."""
    src = make_source_connector(cfg.source)
    tgt = make_target_connector(cfg.target)
    selected_table_names = table_names if table_names is not None else cfg.tables.include

    source = sql_database(
        credentials=src.sqlalchemy_url(_resolve(secrets, cfg.source.secret_id)),
        schema=src.schema_name(),
        table_names=selected_table_names,
        chunk_size=cfg.load.chunk_size,
        backend=cfg.dlt.sql_backend,
    )

    for resource in source.resources.values():
        resource.apply_hints(write_disposition=cfg.load.write_disposition)
    selected = set(selected_table_names)
    selected_overrides = {
        table_name: override
        for table_name, override in cfg.tables.overrides.items()
        if table_name in selected
    }
    _apply_table_overrides(source, selected_overrides)

    dataset_name = derive_dataset_name(
        cfg.target.schema_alias, src.database_name(), src.schema_name()
    )
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name or cfg.pipeline.name,
        destination=tgt.build_destination(_resolve(secrets, cfg.target.secret_id)),
        dataset_name=dataset_name,
    )
    return pipeline, source
