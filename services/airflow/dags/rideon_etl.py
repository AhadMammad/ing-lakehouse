"""Daily Rideon medallion ETL: rideon source → bronze (etl-app) → silver/gold (dbt).

Layout:
    bronze (parallel ingest of 10 source tables via etl-app DockerOperator)
        ↓
    dbt_silver  (dbt build of staging views + silver star tables, on Trino)
        ↓
    dbt_gold    (dbt build of gold marts, on Trino)

Bronze lands raw OLTP into Iceberg exactly like payments. Unlike payments,
silver (star) and gold (marts) are built by dbt-on-Trino, which writes
Iceberg tables into the `silver`/`gold` Nessie namespaces via Trino's
`nessie` catalog. dbt needs only a Trino connection — no S3/Nessie creds.

Idempotent: bronze snapshots overwrite per `logical_date`; dbt models are
`table` materializations (full rebuild each run). max_active_runs=1 keeps
two runs from writing the same Iceberg tables concurrently.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup

from _notifiers import alert_on_failure, alert_on_success
from _telegram_notifiers import telegram_alert_on_failure, telegram_alert_on_success

ETL_IMAGE = os.environ["ETL_APP_IMAGE"]
DBT_IMAGE = os.environ["DBT_IMAGE"]
NETWORK = os.environ["LAKEHOUSE_NETWORK"]
DOCKER_URL = os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock")

# etl-app bronze containers: read the `rideon` DB, write Iceberg via Nessie.
ETL_COMMON = dict(
    image=ETL_IMAGE,
    network_mode=NETWORK,
    auto_remove="success",
    docker_url=DOCKER_URL,
    mount_tmp_dir=False,
    environment={
        "NESSIE_URI": "http://nessie:19120/iceberg",
        "AWS_S3_ENDPOINT": "http://rustfs:9000",
        "AWS_ACCESS_KEY_ID": os.environ["RUSTFS_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["RUSTFS_SECRET_KEY"],
        "ICEBERG_WAREHOUSE_BUCKET": "iceberg-warehouse",
        "PG_HOST": "postgres-source",
        "PG_PORT": "5432",
        "PG_DB": os.environ["RIDEON_SOURCE_DB"],
        "PG_USER": os.environ["POSTGRES_SOURCE_USER"],
        "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
    },
)

# dbt containers: only need a Trino connection (Trino's nessie catalog owns
# the iceberg write path). Internal Trino port is 8080 (8081 is host-only).
DBT_COMMON = dict(
    image=DBT_IMAGE,
    network_mode=NETWORK,
    auto_remove="success",
    docker_url=DOCKER_URL,
    mount_tmp_dir=False,
    environment={
        "TRINO_HOST": "trino",
        "TRINO_PORT": "8080",
        "TRINO_USER": "dbt",
        "TRINO_CATALOG": "nessie",
        "TRINO_SCHEMA": "silver",
    },
)

BRONZE_TABLES = [
    "cities", "vehicle_categories", "riders", "drivers", "vehicles",
    "rides", "fares", "ride_payments", "ratings", "driver_payouts",
]


with DAG(
    dag_id="rideon_etl",
    schedule="0 4 * * *",  # daily 04:00 UTC — after payments (03:00)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rideon", "lakehouse", "medallion", "dbt"],
    description="rideon source → bronze (etl-app) → silver/gold (dbt on Trino)",
    on_failure_callback=[alert_on_failure(), telegram_alert_on_failure()],
    on_success_callback=[alert_on_success(), telegram_alert_on_success()],
) as dag:

    with TaskGroup(group_id="bronze") as bronze:
        for tbl in BRONZE_TABLES:
            DockerOperator(
                task_id=f"ingest_{tbl}",
                command=f"etl_app.jobs.rideon_bronze --table {tbl} --date {{{{ ds }}}}",
                **ETL_COMMON,
            )

    # dbt build runs staging (views) + silver (star tables) together so the
    # views exist before the silver tables compile.
    dbt_silver = DockerOperator(
        task_id="dbt_silver",
        command="build --select path:models/staging path:models/silver --target prod",
        **DBT_COMMON,
    )

    dbt_gold = DockerOperator(
        task_id="dbt_gold",
        command="build --select path:models/gold --target prod",
        **DBT_COMMON,
    )

    bronze >> dbt_silver >> dbt_gold
