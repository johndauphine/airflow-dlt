"""Secrets client interface.

A real Delinea integration would implement this same interface; the mock
implementation (secrets_mock.MockDelineaClient) reads from a local YAML file.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class SecretLookupError(KeyError):
    """Raised when a secret_id is not found in the backing store."""


class SecretsClient(ABC):
    @abstractmethod
    def get(self, secret_id: str) -> dict[str, str]:
        """Return the credential dict for a secret_id.

        Standard keys: ``username``, ``password``. Implementations may
        return additional keys (e.g. ``token``) but callers should not
        assume their presence.
        """
