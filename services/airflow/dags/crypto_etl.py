"""Daily CoinGecko crypto ETL: top-20 markets → bronze/silver/gold Iceberg tables.

Mirrors the weather DAG: three sequential DockerOperator tasks spawn
ephemeral containers from the etl-app image (built per-instance via
`make build-etl-app`) and join the per-instance lakehouse network. Image
tag and network name are read from worker env at parse time.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

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
    dag_id="crypto_etl_top20",
    schedule="0 2 * * *",  # daily 02:00 UTC — staggered from weather (06:00 UTC)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "lakehouse", "medallion"],
    description="CoinGecko top-20 → bronze/silver/gold Iceberg tables",
) as dag:
    bronze = DockerOperator(
        task_id="bronze",
        command="etl_app.jobs.crypto_bronze --date {{ ds }}",
        **COMMON,
    )
    silver = DockerOperator(
        task_id="silver",
        command="etl_app.jobs.crypto_silver --date {{ ds }}",
        **COMMON,
    )
    gold = DockerOperator(
        task_id="gold",
        command="etl_app.jobs.crypto_gold --date {{ ds }}",
        **COMMON,
    )

    bronze >> silver >> gold
