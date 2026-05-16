"""Target dataset (schema) name derivation.

The result becomes dlt's ``dataset_name`` (which becomes the target schema).

Two shapes:
- Source has a schema (MSSQL, Postgres, Oracle, ...): ``{alias}__{db}__{schema}``
- Source has no schema (MySQL, SQLite, ...):           ``{alias}__{db}``

We deliberately do NOT inject a fallback like ``main``/``public`` when no
schema is present — the dataset name should reflect what the source
actually exposes.
"""

from __future__ import annotations

import re

_SANITIZE_RE = re.compile(r"[^a-z0-9_]+")


def _sanitize(part: str) -> str:
    return _SANITIZE_RE.sub("_", part.lower()).strip("_")


def derive_dataset_name(
    alias: str, source_database: str, source_schema: str | None
) -> str:
    """Return the target dataset name (lowercased, sanitized, ``__``-joined)."""
    raw = [alias, source_database]
    if source_schema is not None:
        raw.append(source_schema)
    parts = [_sanitize(p) for p in raw]
    if not all(parts):
        raise ValueError(
            f"alias/database/schema must be non-empty after sanitization: "
            f"alias={alias!r}, db={source_database!r}, schema={source_schema!r}"
        )
    return "__".join(parts)
