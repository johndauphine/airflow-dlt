"""Target dataset (schema) name derivation.

Mirrors the template repo's ``{alias}__{source_db}__{source_schema}`` pattern,
lowercased with underscores. dlt creates the target schema using this name.
"""

from __future__ import annotations

import re

_SANITIZE_RE = re.compile(r"[^a-z0-9_]+")


def _sanitize(part: str) -> str:
    return _SANITIZE_RE.sub("_", part.lower()).strip("_")


def derive_dataset_name(alias: str, source_database: str, source_schema: str) -> str:
    """Return ``{alias}__{source_db}__{source_schema}`` (lowercased, sanitized)."""
    parts = [_sanitize(alias), _sanitize(source_database), _sanitize(source_schema)]
    if not all(parts):
        raise ValueError(
            f"alias/database/schema must be non-empty after sanitization: "
            f"{alias!r}, {source_database!r}, {source_schema!r}"
        )
    return "__".join(parts)
