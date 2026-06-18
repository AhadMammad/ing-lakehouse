"""Daily global earthquake ETL: USGS → bronze → silver → gold Iceberg tables.

Each task spawns an ephemeral container from the etl-app image (built per
instance via `make build-etl-app`) and joins this instance's lakehouse
network. Image tag and network name come from worker env so multi-instance
deployments (one per Unix user via `make init-instance`) stay isolated.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from _notifiers import alert_on_failure, alert_on_success
from _telegram_notifiers import telegram_alert_on_failure, telegram_alert_on_success

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
    },
)

with DAG(
    dag_id="earthquake_etl_global",
    schedule="0 7 * * *",  # daily 07:00 UTC — after weather ETL (06:00)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["earthquake", "lakehouse", "medallion"],
    description="USGS global earthquakes (M≥1.0) → bronze/silver/gold Iceberg tables",
    on_failure_callback=[alert_on_failure(), telegram_alert_on_failure()],
    on_success_callback=[alert_on_success(), telegram_alert_on_success()],
) as dag:
    bronze = DockerOperator(
        task_id="bronze",
        command="etl_app.jobs.earthquake_bronze --date {{ ds }}",
        **COMMON,
    )
    silver = DockerOperator(
        task_id="silver",
        command="etl_app.jobs.earthquake_silver --date {{ ds }}",
        **COMMON,
    )
    gold = DockerOperator(
        task_id="gold",
        command="etl_app.jobs.earthquake_gold --date {{ ds }}",
        **COMMON,
    )

    bronze >> silver >> gold
