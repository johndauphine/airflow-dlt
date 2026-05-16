"""Mock Delinea client backed by a local YAML file.

File format::

    secrets:
      <secret_id>:
        username: ...
        password: ...
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from airflow_dlt.secrets import SecretLookupError, SecretsClient


class MockDelineaClient(SecretsClient):
    def __init__(self, secrets_file: Path):
        self._path = Path(secrets_file)
        raw: Any = yaml.safe_load(self._path.read_text())
        if not isinstance(raw, dict) or "secrets" not in raw:
            raise ValueError(
                f"{self._path}: expected top-level 'secrets' mapping"
            )
        self._data: dict[str, dict[str, str]] = raw["secrets"] or {}

    def get(self, secret_id: str) -> dict[str, str]:
        if secret_id not in self._data:
            raise SecretLookupError(
                f"secret_id {secret_id!r} not found in {self._path}"
            )
        return dict(self._data[secret_id])
