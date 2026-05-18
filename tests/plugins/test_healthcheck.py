from datetime import datetime, timezone

from sqlalchemy import create_engine, text

import airflow_dlt.healthcheck as healthcheck
from airflow_dlt.healthcheck import DagFreshness, evaluate_dag_freshness


def test_evaluate_dag_freshness_accepts_fresh_dags():
    errors = evaluate_dag_freshness(
        [DagFreshness("dlt_pipeline", age_seconds=12, is_stale=False)],
        ["dlt_pipeline"],
        max_age_seconds=300,
    )

    assert errors == []


def test_evaluate_dag_freshness_reports_missing_stale_and_old_dags():
    errors = evaluate_dag_freshness(
        [
            DagFreshness("old_dag", age_seconds=301, is_stale=False),
            DagFreshness("stale_dag", age_seconds=12, is_stale=True),
        ],
        ["old_dag", "stale_dag", "missing_dag"],
        max_age_seconds=300,
    )

    assert errors == [
        "old_dag: last parsed 301s ago (max 300s)",
        "stale_dag: marked stale",
        "missing_dag: missing from dag table",
    ]


def test_fetch_dag_freshness_maps_rows_with_python_age_calculation():
    engine = create_engine("sqlite:///:memory:")
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                "create table dag ("
                "dag_id text primary key, "
                "is_stale boolean, "
                "last_parsed_time timestamp)"
            )
        )
        conn.execute(
            text(
                "insert into dag (dag_id, is_stale, last_parsed_time) "
                "values (:dag_id, :is_stale, :last_parsed_time)"
            ),
            {
                "dag_id": "dlt_pipeline",
                "is_stale": False,
                "last_parsed_time": "2026-05-18T11:59:15+00:00",
            },
        )

    rows = healthcheck._fetch_dag_freshness(
        ["dlt_pipeline", "missing_dag"],
        engine=engine,
        now=now,
    )

    assert rows == [
        DagFreshness("dlt_pipeline", age_seconds=45.0, is_stale=False)
    ]


def test_main_reports_success(monkeypatch, capsys):
    monkeypatch.setattr(
        healthcheck,
        "_fetch_dag_freshness",
        lambda dag_ids: [DagFreshness(dag_ids[0], age_seconds=12, is_stale=False)],
    )

    result = healthcheck.main(
        ["dag-processor-fresh", "--dag-id", "dlt_pipeline", "--max-age-seconds", "300"]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "dag processor freshness ok: dlt_pipeline age=12s" in captured.out


def test_main_reports_failure(monkeypatch, capsys):
    monkeypatch.setattr(
        healthcheck,
        "_fetch_dag_freshness",
        lambda dag_ids: [DagFreshness(dag_ids[0], age_seconds=301, is_stale=False)],
    )

    result = healthcheck.main(
        ["dag-processor-fresh", "--dag-id", "dlt_pipeline", "--max-age-seconds", "300"]
    )

    captured = capsys.readouterr()
    assert result == 1
    assert "dag processor freshness check failed:" in captured.err
    assert "dlt_pipeline: last parsed 301s ago (max 300s)" in captured.err
