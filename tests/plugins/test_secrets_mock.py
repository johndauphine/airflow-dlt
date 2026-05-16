import pytest

from airflow_dlt.secrets_client import SecretLookupError
from airflow_dlt.secrets_mock import MockDelineaClient


SECRETS_YAML = """
secrets:
  src_db:
    username: sa
    password: hunter2
  tgt_db:
    username: postgres
    password: pg-pass
"""


def test_get_returns_credential_dict(tmp_path):
    f = tmp_path / "secrets.yaml"
    f.write_text(SECRETS_YAML)
    client = MockDelineaClient(f)

    assert client.get("src_db") == {"username": "sa", "password": "hunter2"}
    assert client.get("tgt_db")["username"] == "postgres"


def test_unknown_secret_raises(tmp_path):
    f = tmp_path / "secrets.yaml"
    f.write_text(SECRETS_YAML)
    client = MockDelineaClient(f)

    with pytest.raises(SecretLookupError):
        client.get("missing_id")


def test_returned_dict_is_isolated(tmp_path):
    """Mutating the returned dict must not affect the internal store."""
    f = tmp_path / "secrets.yaml"
    f.write_text(SECRETS_YAML)
    client = MockDelineaClient(f)

    creds = client.get("src_db")
    creds["password"] = "TAMPERED"
    assert client.get("src_db")["password"] == "hunter2"


def test_malformed_file_rejected(tmp_path):
    f = tmp_path / "secrets.yaml"
    f.write_text("not_secrets: {}\n")
    with pytest.raises(ValueError):
        MockDelineaClient(f)
