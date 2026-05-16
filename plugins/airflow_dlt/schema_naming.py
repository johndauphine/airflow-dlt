"""Target dataset (schema) name derivation.

The result becomes dlt's ``dataset_name`` (which becomes the target schema in
postgres, the dataset file suffix in sqlite, etc.).

Two shapes:
- Source has a schema (MSSQL, Postgres, Oracle, ...): ``{alias}_{db}_{schema}``
- Source has no schema (MySQL, SQLite, ...):           ``{alias}_{db}``

We use a single underscore between segments rather than ``__`` because dlt's
postgres destination normalizes consecutive underscores in schema names —
``test__app__public`` would silently become ``test_app_public`` in the
actual destination, so ``pipeline.dataset_name`` would diverge from the
schema you'd query with ``\\dn``. Using ``_`` keeps the two aligned.

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
    """Return the target dataset name (lowercased, sanitized, ``_``-joined)."""
    raw = [alias, source_database]
    if source_schema is not None:
        raw.append(source_schema)
    parts = [_sanitize(p) for p in raw]
    if not all(parts):
        raise ValueError(
            f"alias/database/schema must be non-empty after sanitization: "
            f"alias={alias!r}, db={source_database!r}, schema={source_schema!r}"
        )
    return "_".join(parts)
