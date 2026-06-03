"""Rideon source seeder DAG.

Triggers the data-generator container to populate the `rideon` database in
postgres-source with fake ride-hailing data for a given date range. Manually
triggered only — pass start_date, end_date, and optionally rides_per_day.

Example trigger via Airflow UI:
  {"start_date": "2024-01-01", "end_date": "2024-01-31", "rides_per_day": 500}

Or via CLI:
  airflow dags trigger rideon_source_seeder \
    --conf '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback

DATA_GEN_IMAGE = os.environ["DATA_GENERATOR_IMAGE"]
NETWORK        = os.environ["LAKEHOUSE_NETWORK"]

log = logging.getLogger(__name__)


def _deadline_breach(**kwargs):
    ctx = kwargs.get("context", {})
    dag_run = ctx.get("dag_run")
    log.warning(
        "rideon_source_seeder exceeded deadline | dag_run=%s conf=%s",
        getattr(dag_run, "run_id", dag_run),
        getattr(dag_run, "conf", None),
    )


with DAG(
    dag_id="rideon_source_seeder",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rideon", "source", "seeder"],
    description="Seed the rideon DB with fake ride-hailing data for a date range",
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=SyncCallback(_deadline_breach),
    ),
) as dag:
    DockerOperator(
        task_id="seed",
        image=DATA_GEN_IMAGE,
        command=(
            "data_generator.rideon"
            " --start-date {{ dag_run.conf['start_date'] }}"
            " --end-date {{ dag_run.conf['end_date'] }}"
            " --rides-per-day {{ dag_run.conf.get('rides_per_day', 500) }}"
        ),
        network_mode=NETWORK,
        auto_remove="success",
        docker_url=os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock"),
        mount_tmp_dir=False,
        environment={
            "PG_HOST":     "postgres-source",
            "PG_PORT":     "5432",
            "PG_DB":       os.environ["RIDEON_SOURCE_DB"],
            "PG_USER":     os.environ["POSTGRES_SOURCE_USER"],
            "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
        },
    )
