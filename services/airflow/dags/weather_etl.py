"""Daily Baku weather ETL: Open-Meteo → bronze → silver → gold Iceberg tables.

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

# Resolved per-instance by Compose substitution into the worker's env
# (see services/airflow/docker-compose.yml x-airflow-common.environment).
ETL_IMAGE = os.environ["ETL_APP_IMAGE"]
NETWORK = os.environ["LAKEHOUSE_NETWORK"]

COMMON = dict(
    image=ETL_IMAGE,
    network_mode=NETWORK,
    auto_remove="success",  # keep failed containers around for debug
    # DOCKER_HOST is injected by the worker env (points at docker-socket-proxy).
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
    dag_id="weather_etl_baku",
    schedule="0 6 * * *",  # daily 06:00 UTC — after Open-Meteo refresh
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "lakehouse", "medallion"],
    description="Open-Meteo Baku → bronze/silver/gold Iceberg tables",
) as dag:
    bronze = DockerOperator(
        task_id="bronze",
        command="etl_app.jobs.bronze --date {{ ds }}",
        **COMMON,
    )
    silver = DockerOperator(
        task_id="silver",
        command="etl_app.jobs.silver --date {{ ds }}",
        **COMMON,
    )
    gold = DockerOperator(
        task_id="gold",
        command="etl_app.jobs.gold --date {{ ds }}",
        **COMMON,
    )

    bronze >> silver >> gold
