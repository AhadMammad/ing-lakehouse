"""Daily auth medallion ETL: postgres-source `auth` DB → Iceberg via Spark.

Layout:
    bronze (parallel ingest of 13 source tables)
        ↓
    silver (parallel cleaned/upserted tables)
        ↓
    gold
        dimensions  (serial — dim_user SCD2 must have no concurrent writer)
            ↓
        facts       (parallel)

Unlike payments (PyIceberg DockerOperator) and rideon (dbt-on-Trino), every
layer here runs as an OOP PySpark job on the standalone Spark cluster. The
Airflow worker has no JDK/Spark client, so each task is a BashOperator that
`docker exec`s spark-submit into the spark-master container (client mode:
driver in master, executors on the 2 workers). The custom Spark image bakes
the jars + the etl_app_spark code, so no --packages / --py-files are needed.

Idempotent: bronze snapshots overwrite per `logical_date`, silver upserts on
natural keys, gold dims MERGE (dim_user as SCD2), gold facts MERGE on PK.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

from _notifiers import alert_on_failure, alert_on_success
from _telegram_notifiers import telegram_alert_on_failure, telegram_alert_on_success

SPARK_CONTAINER = os.environ["SPARK_MASTER_CONTAINER"]
APP = "/opt/etl_app_spark/etl_app_spark/__main__.py"

# Env passed into the spark-submit via `docker exec -e ...` so the job is
# self-contained regardless of the spark container's own env. Values come from
# the Airflow worker env (Compose populates them from the active .env.<instance>).
_ENV = {
    "PG_HOST": "postgres-source",
    "PG_PORT": "5432",
    "PG_DB": os.environ.get("AUTH_SOURCE_DB", "auth"),
    "PG_USER": os.environ["POSTGRES_SOURCE_USER"],
    "PG_PASSWORD": os.environ["POSTGRES_SOURCE_PASSWORD"],
    "NESSIE_URI": "http://nessie:19120/iceberg",
    "AWS_S3_ENDPOINT": "http://rustfs:9000",
    "AWS_ACCESS_KEY_ID": os.environ["RUSTFS_ACCESS_KEY"],
    "AWS_SECRET_ACCESS_KEY": os.environ["RUSTFS_SECRET_KEY"],
    "ICEBERG_WAREHOUSE_BUCKET": "iceberg-warehouse",
}
_ENV_FLAGS = " ".join(f"-e {k}={v}" for k, v in _ENV.items())


def spark_submit(job: str, table: str) -> str:
    """BashOperator command: docker exec spark-submit into spark-master (client mode).

    spark-submit's exit code propagates back through `docker exec` (and the
    docker-socket-proxy), so a failed Spark job correctly fails the task.
    """
    return (
        f"docker exec {_ENV_FLAGS} {SPARK_CONTAINER} "
        f"/opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 --deploy-mode client "
        f"--name auth_{job}_{table}_{{{{ ds }}}} "
        f"{APP} --job {job} --table {table} --date {{{{ ds }}}} --run-id '{{{{ run_id }}}}'"
    )


BRONZE_TABLES = [
    "users", "credentials", "user_profiles", "roles", "permissions",
    "role_permissions", "user_roles", "sessions", "login_attempts",
    "oauth_accounts", "mfa_devices", "password_reset_tokens", "audit_log", 
]
SILVER_TABLES = [
    "users", "roles", "permissions", "sessions",
    "login_attempts", "oauth_accounts", "mfa_devices",
]
# dim_user last so its SCD2 close-out/insert has no concurrent dim writer.
GOLD_DIMS = ["dim_date", "dim_role", "dim_auth_method", "dim_user"]
GOLD_FACTS = ["fact_login_attempts", "fact_sessions"]


with DAG(
    dag_id="auth_etl",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    # Local Spark cluster is small (2 workers × 2 cores). Each task is its own
    # spark-submit, so run ONE task at a time across the whole DAG — the bronze
    # and silver TaskGroups no longer fan out in parallel. The dependency graph
    # (bronze → silver → gold, dims serial, dims → facts) is unchanged; only the
    # in-group concurrency drops to 1.
    max_active_tasks=1,
    tags=["auth", "lakehouse", "medallion", "spark"],
    description="postgres-source auth → auth_bronze/silver/gold(star, dim_user SCD2) via Spark",
    on_failure_callback=[alert_on_failure(), telegram_alert_on_failure()],
    on_success_callback=[alert_on_success(), telegram_alert_on_success()],
) as dag:

    with TaskGroup(group_id="bronze") as bronze:
        for tbl in BRONZE_TABLES:
            BashOperator(task_id=f"ingest_{tbl}", bash_command=spark_submit("bronze", tbl))

    with TaskGroup(group_id="silver") as silver:
        for tbl in SILVER_TABLES:
            BashOperator(task_id=tbl, bash_command=spark_submit("silver", tbl))

    with TaskGroup(group_id="gold") as gold:
        with TaskGroup(group_id="dimensions") as dims:
            prev = None
            for d in GOLD_DIMS:
                op = BashOperator(task_id=d, bash_command=spark_submit("gold", d))
                if prev is not None:
                    prev >> op
                prev = op
        with TaskGroup(group_id="facts") as facts:
            for f in GOLD_FACTS:
                BashOperator(task_id=f, bash_command=spark_submit("gold", f))
        dims >> facts

    bronze >> silver >> gold
