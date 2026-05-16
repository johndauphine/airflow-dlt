"""SQLite → SQLite end-to-end smoke test.

Deliberately UNMARKED — runs as part of the default test suite, since SQLite
needs no external infra. This is the regression guard for the connector
framework and dlt's load semantics that runs anywhere Python + dlt run, with
no Docker, no Postgres, no MSSQL.

The MSSQL→Postgres and Postgres→Postgres tests stay marked
``@pytest.mark.integration`` because they need containers; this one doesn't.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("dlt")

from airflow_dlt.config import PipelineConfig
from airflow_dlt.dlt_pipeline import build_pipeline
from airflow_dlt.secrets_client import SecretsClient


class _NoopSecrets(SecretsClient):
    """For pipelines whose endpoints (SQLite) don't need credentials."""

    def get(self, secret_id: str) -> dict[str, str]:  # pragma: no cover
        raise AssertionError(
            f"SQLite endpoints should not request secrets; was asked for {secret_id!r}"
        )


def _seed_source(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)"
        )
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL)"
        )
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?, ?)",
            [(1, "alice", 9.5), (2, "bob", 7.0), (3, "carol", 8.25)],
        )
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(1, 1, 19.99), (2, 1, 4.50), (3, 2, 100.00), (4, 3, 7.25)],
        )
        conn.commit()
    finally:
        conn.close()


def _config(src_path: Path, tgt_path: Path, *, name: str) -> PipelineConfig:
    return PipelineConfig.model_validate({
        "pipeline": {"name": name},
        "source": {"type": "sqlite", "path": str(src_path)},
        "target": {"type": "sqlite", "path": str(tgt_path), "schema_alias": "test"},
        "tables": {"include": ["customers", "orders"]},
        "load": {"write_disposition": "replace", "chunk_size": 1000},
    })


def test_sqlite_to_sqlite_replace(tmp_path):
    """Seeded SQLite rows must land in the dataset's SQLite file."""
    src = tmp_path / "source.db"
    tgt = tmp_path / "target.db"
    _seed_source(src)

    cfg = _config(src, tgt, name="sqlite_smoke_replace")
    pipeline, source = build_pipeline(cfg, _NoopSecrets())
    load_info = pipeline.run(source)
    assert not load_info.has_failed_jobs, f"dlt reported failed jobs: {load_info}"

    counts = _dataset_table_row_counts(tmp_path, tgt)
    assert counts.get("customers") == 3, f"customers ≠ 3, full counts: {counts}"
    assert counts.get("orders") == 4, f"orders ≠ 4, full counts: {counts}"


def test_sqlite_to_sqlite_replace_is_idempotent(tmp_path):
    """A second run with `replace` must not double-write."""
    src = tmp_path / "source.db"
    tgt = tmp_path / "target.db"
    _seed_source(src)

    cfg = _config(src, tgt, name="sqlite_smoke_idempotent")
    p1, s1 = build_pipeline(cfg, _NoopSecrets())
    p1.run(s1)
    p2, s2 = build_pipeline(cfg, _NoopSecrets())
    p2.run(s2)

    counts = _dataset_table_row_counts(tmp_path, tgt)
    assert counts.get("customers") == 3
    assert counts.get("orders") == 4


# ---------------------------------------------------------------------------
# helpers — dlt's sqlalchemy/SQLite destination puts each dataset in its own
# file named ``{target_stem}__{normalized_dataset_name}.db`` next to the
# target file; the user-supplied target.db only holds state. Find the data
# file by globbing siblings.
# ---------------------------------------------------------------------------

def _dataset_table_row_counts(tmp_dir: Path, target_anchor: Path) -> dict[str, int]:
    """Return {table_name: row_count} from the dlt-produced dataset file.

    We don't hardcode the dlt dataset-file naming scheme (which has shifted
    across versions and includes dataset-name normalization); instead we
    pick the largest sibling .db file that isn't the source or the anchor.
    """
    candidates = [
        p for p in tmp_dir.glob("*.db")
        if p.name not in {"source.db", target_anchor.name}
    ]
    if not candidates:
        raise AssertionError(
            f"no dlt dataset file produced; tmp_dir contains: {[p.name for p in tmp_dir.iterdir()]}"
        )
    data_file = max(candidates, key=lambda p: p.stat().st_size)
    conn = sqlite3.connect(data_file)
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {
            name: conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            for (name,) in cur.fetchall()
        }
    finally:
        conn.close()
