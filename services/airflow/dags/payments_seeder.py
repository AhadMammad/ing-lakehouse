"""Payments source seeder DAG.

Triggers the data-generator container to populate postgres-source with
fake payments platform data for a given date range. Manually triggered
only — pass start_date, end_date, and optionally rows_per_day in conf.

Example trigger via Airflow UI:
  {"start_date": "2024-01-01", "end_date": "2024-01-31", "rows_per_day": 1000}

Or via CLI:
  airflow dags trigger payments_source_seeder \
    --conf '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

DATA_GEN_IMAGE = os.environ["DATA_GENERATOR_IMAGE"]
NETWORK        = os.environ["LAKEHOUSE_NETWORK"]

with DAG(
    dag_id="payments_source_seeder",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "source", "seeder"],
    description="Seed postgres-source with fake payments platform data for a date range",
) as dag:
    DockerOperator(
        task_id="seed",
        image=DATA_GEN_IMAGE,
        command=(
            "data_generator"
            " --start-date {{ dag_run.conf['start_date'] }}"
            " --end-date {{ dag_run.conf['end_date'] }}"
            " --rows-per-day {{ dag_run.conf.get('rows_per_day', 1000) }}"
        ),
        network_mode=NETWORK,
        auto_remove="success",
        docker_url=os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock"),
        mount_tmp_dir=False,
        environment={
            "PG_HOST":     "postgres-source",
            "PG_PORT":     "5432",
            "PG_DB":       os.environ["POSTGRES_SOURCE_DB"],
            "PG_USER":     os.environ["POSTGRES_SOURCE_USER"],
            "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
        },
    )
