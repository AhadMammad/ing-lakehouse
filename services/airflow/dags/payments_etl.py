"""Daily payments medallion ETL: postgres-source → bronze/silver/gold Iceberg.

Layout:
    bronze (parallel ingest of 7 source tables)
        ↓
    silver
        dimensions  (parallel)
            ↓
        facts       (parallel)
        ↓
    gold (parallel marts)

Idempotent: bronze snapshots overwrite per `logical_date`, silver upserts on
natural keys, gold marts overwrite per partition (or full-table for CLV).
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup

ETL_IMAGE = os.environ["ETL_APP_IMAGE"]
NETWORK = os.environ["LAKEHOUSE_NETWORK"]

COMMON = dict(
    image=ETL_IMAGE,
    network_mode=NETWORK,
    auto_remove="success",
    docker_url=os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock"),
    mount_tmp_dir=False,
    environment={
        "NESSIE_URI": "http://nessie:19120/iceberg",
        "AWS_S3_ENDPOINT": "http://rustfs:9000",
        "AWS_ACCESS_KEY_ID": os.environ["RUSTFS_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["RUSTFS_SECRET_KEY"],
        "ICEBERG_WAREHOUSE_BUCKET": "iceberg-warehouse",
        "PG_HOST": "postgres-source",
        "PG_PORT": "5432",
        "PG_DB": os.environ["POSTGRES_SOURCE_DB"],
        "PG_USER": os.environ["POSTGRES_SOURCE_USER"],
        "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
    },
)

BRONZE_TABLES = [
    "merchants", "customers", "methods",
    "payments", "refunds", "fees", "settlements",
]
SILVER_DIMS = ["dim_date", "dim_customer", "dim_merchant", "dim_payment_method"]
SILVER_FACTS = ["fact_payments", "fact_refunds", "fact_fees", "fact_settlements"]
GOLD_MARTS = [
    "daily_payment_summary", "daily_revenue_by_merchant",
    "customer_lifetime_value", "merchant_settlement_daily",
]


with DAG(
    dag_id="payments_etl",
    schedule="0 3 * * *",  # daily 03:00 UTC — staggered after crypto (02:00) and before weather (06:00)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "lakehouse", "medallion"],
    description="postgres-source payments → bronze/silver(star)/gold Iceberg tables",
) as dag:

    with TaskGroup(group_id="bronze") as bronze:
        for tbl in BRONZE_TABLES:
            DockerOperator(
                task_id=f"ingest_{tbl}",
                command=f"etl_app.jobs.payments_bronze --table {tbl} --date {{{{ ds }}}}",
                **COMMON,
            )

    with TaskGroup(group_id="silver") as silver:
        with TaskGroup(group_id="dimensions") as dims:
            for d in SILVER_DIMS:
                DockerOperator(
                    task_id=d,
                    command=f"etl_app.jobs.payments_silver --table {d} --date {{{{ ds }}}}",
                    **COMMON,
                )
        with TaskGroup(group_id="facts") as facts:
            for f in SILVER_FACTS:
                DockerOperator(
                    task_id=f,
                    command=f"etl_app.jobs.payments_silver --table {f} --date {{{{ ds }}}}",
                    **COMMON,
                )
        dims >> facts

    with TaskGroup(group_id="gold") as gold:
        for m in GOLD_MARTS:
            DockerOperator(
                task_id=m,
                command=f"etl_app.jobs.payments_gold --mart {m} --date {{{{ ds }}}}",
                **COMMON,
            )

    bronze >> silver >> gold
