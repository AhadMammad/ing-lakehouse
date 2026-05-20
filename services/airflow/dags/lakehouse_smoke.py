"""Lakehouse smoke test — producer / consumer pair wired through an Asset.

Producer (lakehouse_smoke_producer): writes a marker object to RustFS via
aws_default and declares it as an outlet Asset. Manual trigger only.

Consumer (lakehouse_smoke_consumer): scheduled on the Asset. Runs
SELECT 1 against Trino via trino_default and validates the result.

Together these prove three things end-to-end against the running stack:
  - apache-airflow-providers-amazon talks to RustFS (S3 endpoint override)
  - apache-airflow-providers-trino talks to Trino
  - Airflow 3.x Asset-based DAG-to-DAG triggering works
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, Asset, task

SMOKE_MARKER = Asset("s3://lakehouse/airflow/_smoke")


with DAG(
    dag_id="lakehouse_smoke_producer",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smoke", "lakehouse"],
    description="Writes a smoke-test marker to RustFS and emits the lakehouse Asset.",
) as producer:

    @task
    def announce(**context) -> str:
        # `ds` only exists when the run has a logical_date; manual triggers
        # without one omit it. Use the always-present run_id.
        return f"producer run_id={context['run_id']}"

    write_marker = S3CreateObjectOperator(
        task_id="write_marker",
        aws_conn_id="aws_default",
        s3_bucket="lakehouse",
        s3_key="airflow/_smoke",
        data=b"ok",
        replace=True,
        outlets=[SMOKE_MARKER],
    )

    announce() >> write_marker


with DAG(
    dag_id="lakehouse_smoke_consumer",
    schedule=[SMOKE_MARKER],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smoke", "lakehouse"],
    description="Triggered by the smoke Asset; runs SELECT 1 against Trino.",
) as consumer:

    check_trino = SQLExecuteQueryOperator(
        task_id="check_trino",
        conn_id="trino_default",
        sql="SELECT 1 AS smoke",
        do_xcom_push=True,
    )

    @task
    def validate(rows) -> None:
        assert rows == [[1]] or rows == [(1,)], f"unexpected: {rows!r}"

    validate(check_trino.output)
