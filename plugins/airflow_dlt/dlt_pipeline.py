"""Construct a dlt Pipeline + source from a validated YAML config.

This is the bridge between our config layer and dlt. ``build_pipeline``
returns a ready-to-run ``(pipeline, source)`` pair; the caller invokes
``pipeline.run(source)`` to execute the load.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy.engine.url import URL

from airflow_dlt.config import PipelineConfig, TableOverride
from airflow_dlt.schema_naming import derive_dataset_name
from airflow_dlt.secrets_client import SecretsClient


def _build_mssql_url(cfg: PipelineConfig, creds: dict[str, str]) -> URL:
    """SQLAlchemy URL for ``mssql+pyodbc`` with options as ODBC connect args."""
    query: dict[str, str] = {"driver": cfg.source.driver, **cfg.source.options}
    return URL.create(
        drivername="mssql+pyodbc",
        username=creds["username"],
        password=creds["password"],
        host=cfg.source.host,
        port=cfg.source.port,
        database=cfg.source.database,
        query=query,
    )


def _build_postgres_credentials(cfg: PipelineConfig, creds: dict[str, str]) -> str:
    """Postgres credentials string accepted by ``dlt.destinations.postgres``.

    Uses RFC 3986 percent-encoding (``quote``) rather than form-encoding
    (``quote_plus``): SQLAlchemy parses URL userinfo per RFC 3986, where a
    literal ``+`` is *not* a decoded space, so ``quote_plus`` would silently
    corrupt credentials containing spaces.
    """
    user = quote(creds["username"], safe="")
    pw = quote(creds["password"], safe="")
    return f"postgresql://{user}:{pw}@{cfg.target.host}:{cfg.target.port}/{cfg.target.database}"


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
            hints["incremental"] = dlt.sources.incremental(
                cursor_path=override.incremental.cursor_path,
                initial_value=override.incremental.initial_value,
            )
        if hints:
            resource.apply_hints(**hints)


def build_pipeline(cfg: PipelineConfig, secrets: SecretsClient) -> tuple[Any, Any]:
    """Return a ``(pipeline, source)`` pair ready for ``pipeline.run(source)``."""
    source_url = _build_mssql_url(cfg, secrets.get(cfg.source.secret_id))
    target_creds = _build_postgres_credentials(cfg, secrets.get(cfg.target.secret_id))

    # dlt's sql_database wants a connection-string str, not a SQLAlchemy URL
    # object. Render with the password visible so dlt can parse it.
    source = sql_database(
        credentials=source_url.render_as_string(hide_password=False),
        schema=cfg.source.schema_,
        table_names=cfg.tables.include,
        chunk_size=cfg.load.chunk_size,
    )

    for resource in source.resources.values():
        resource.apply_hints(write_disposition=cfg.load.write_disposition)
    _apply_table_overrides(source, cfg.tables.overrides)

    dataset_name = derive_dataset_name(
        cfg.target.schema_alias, cfg.source.database, cfg.source.schema_
    )
    pipeline = dlt.pipeline(
        pipeline_name=cfg.pipeline.name,
        destination=dlt.destinations.postgres(credentials=target_creds),
        dataset_name=dataset_name,
    )
    return pipeline, source
