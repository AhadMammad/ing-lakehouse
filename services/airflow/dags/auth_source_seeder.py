"""Auth source seeder DAG.

Triggers the data-generator container to backfill the `auth` database in
postgres-source with fake identity-service data for a date range. Manually
triggered only (no schedule).

Defaults match `make run-auth-generator START=2026-05-01 END=2026-05-07 USERS=50`,
so a plain "Trigger DAG" with no config seeds 2026-05-01 → 2026-05-07 at 50
new users/day. Override any of them at trigger time, e.g.:

  {"start_date": "2026-05-01", "end_date": "2026-05-31", "users_per_day": 100}

Or via CLI:
  airflow dags trigger auth_source_seeder \
    --conf '{"start_date": "2026-05-01", "end_date": "2026-05-07", "users_per_day": 50}'

The generator seeds the static RBAC pool + initial user cohort once
(idempotent) and skips any day that already has login_attempts, so re-running
the same range is safe.
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
        "auth_source_seeder exceeded deadline | dag_run=%s conf=%s",
        getattr(dag_run, "run_id", dag_run),
        getattr(dag_run, "conf", None),
    )


with DAG(
    dag_id="auth_source_seeder",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["auth", "source", "seeder"],
    description="Backfill the auth DB with fake identity-service data for a date range",
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
            "data_generator.auth"
            " --start-date {{ dag_run.conf.get('start_date', '2026-05-01') }}"
            " --end-date {{ dag_run.conf.get('end_date', '2026-05-07') }}"
            " --users-per-day {{ dag_run.conf.get('users_per_day', 50) }}"
        ),
        network_mode=NETWORK,
        auto_remove="success",
        docker_url=os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock"),
        mount_tmp_dir=False,
        environment={
            "PG_HOST":     "postgres-source",
            "PG_PORT":     "5432",
            "PG_DB":       os.environ.get("AUTH_SOURCE_DB", "auth"),
            "PG_USER":     os.environ["POSTGRES_SOURCE_USER"],
            "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
        },
    )
