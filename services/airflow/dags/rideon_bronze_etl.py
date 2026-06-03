"""Rideon bronze-only ETL: rideon source → bronze (etl-app).

Manually triggered so it can be run on-demand to populate the `bronze`
Nessie namespace before `rideon_etl` or `make run-dbt` is invoked.
All 10 source tables are ingested in parallel for a single date.

Example trigger via Airflow UI:
  {"date": "2026-05-27"}

Or via CLI:
  airflow dags trigger rideon_bronze_etl --conf '{"date": "2026-05-27"}'

If no `date` is supplied in conf the DAG falls back to the logical date (ds).
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, TaskGroup

ETL_IMAGE  = os.environ["ETL_APP_IMAGE"]
NETWORK    = os.environ["LAKEHOUSE_NETWORK"]
DOCKER_URL = os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock")

ETL_COMMON = dict(
    image=ETL_IMAGE,
    network_mode=NETWORK,
    auto_remove="success",
    docker_url=DOCKER_URL,
    mount_tmp_dir=False,
    environment={
        "NESSIE_URI":               "http://nessie:19120/iceberg",
        "AWS_S3_ENDPOINT":          "http://rustfs:9000",
        "AWS_ACCESS_KEY_ID":        os.environ["RUSTFS_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY":    os.environ["RUSTFS_SECRET_KEY"],
        "ICEBERG_WAREHOUSE_BUCKET": "iceberg-warehouse",
        "PG_HOST":                  "postgres-source",
        "PG_PORT":                  "5432",
        "PG_DB":                    os.environ["RIDEON_SOURCE_DB"],
        "PG_USER":                  os.environ["POSTGRES_SOURCE_USER"],
        "PG_PASSWORD":              os.environ["POSTGRES_SOURCE_PASSWORD"],
    },
)

BRONZE_TABLES = [
    "cities", "vehicle_categories", "riders", "drivers", "vehicles",
    "rides", "fares", "ride_payments", "ratings", "driver_payouts",
]

# Use conf["date"] when supplied, otherwise fall back to the logical date.
_DATE_EXPR = "{{ dag_run.conf.get('date', ds) }}"

with DAG(
    dag_id="rideon_bronze_etl",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rideon", "bronze", "etl"],
    description="Ingest rideon source tables into bronze Iceberg (manual trigger)",
) as dag:

    with TaskGroup(group_id="bronze") as bronze:
        for tbl in BRONZE_TABLES:
            DockerOperator(
                task_id=f"ingest_{tbl}",
                command=f"etl_app.jobs.rideon_bronze --table {tbl} --date {_DATE_EXPR}",
                **ETL_COMMON,
            )
