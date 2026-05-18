"""Small health checks for local Airflow services."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DagFreshness:
    dag_id: str
    age_seconds: float | None
    is_stale: bool | None


def evaluate_dag_freshness(
    rows: Iterable[DagFreshness],
    expected_dag_ids: Iterable[str],
    max_age_seconds: int,
) -> list[str]:
    """Return human-readable freshness failures for expected DAG rows."""
    by_id = {row.dag_id: row for row in rows}
    errors: list[str] = []
    for dag_id in expected_dag_ids:
        row = by_id.get(dag_id)
        if row is None:
            errors.append(f"{dag_id}: missing from dag table")
            continue
        if row.age_seconds is None:
            errors.append(f"{dag_id}: last_parsed_time is null")
        elif row.age_seconds > max_age_seconds:
            errors.append(
                f"{dag_id}: last parsed {row.age_seconds:.0f}s ago "
                f"(max {max_age_seconds}s)"
            )
        if row.is_stale:
            errors.append(f"{dag_id}: marked stale")
    return errors


def _database_url() -> str:
    url = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not url:
        raise RuntimeError("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is not set")
    return url


def _as_utc_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _fetch_dag_freshness(
    dag_ids: list[str],
    *,
    engine: Engine | None = None,
    now: datetime | None = None,
) -> list[DagFreshness]:
    if not dag_ids:
        return []

    placeholders = ", ".join(f":dag_{i}" for i in range(len(dag_ids)))
    params = {f"dag_{i}": dag_id for i, dag_id in enumerate(dag_ids)}
    query = text(
        f"select dag_id, is_stale, last_parsed_time from dag where dag_id in ({placeholders})"
    )
    owns_engine = engine is None
    if engine is None:
        engine = create_engine(_database_url())
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
    finally:
        if owns_engine:
            engine.dispose()

    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    freshness: list[DagFreshness] = []
    for row in rows:
        last_parsed_time = _as_utc_datetime(row["last_parsed_time"])
        age_seconds = (
            (now_utc - last_parsed_time).total_seconds()
            if last_parsed_time is not None
            else None
        )
        freshness.append(
            DagFreshness(
                dag_id=str(row["dag_id"]),
                age_seconds=age_seconds,
                is_stale=(
                    bool(row["is_stale"]) if row["is_stale"] is not None else None
                ),
            )
        )
    return freshness


def _check_dag_processor_fresh(args: argparse.Namespace) -> int:
    rows = _fetch_dag_freshness(args.dag_id)
    errors = evaluate_dag_freshness(rows, args.dag_id, args.max_age_seconds)
    if errors:
        print("dag processor freshness check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(
        "dag processor freshness ok: "
        + ", ".join(f"{row.dag_id} age={row.age_seconds:.0f}s" for row in rows)
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    dag_processor = subparsers.add_parser("dag-processor-fresh")
    dag_processor.add_argument("--dag-id", action="append", required=True)
    dag_processor.add_argument("--max-age-seconds", type=int, default=300)
    dag_processor.set_defaults(func=_check_dag_processor_fresh)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
