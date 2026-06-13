"""Entry point — dispatch a medallion job to its Spark job class.

Invoked by spark-submit (inside the spark-master container via the Airflow
auth_etl DAG):

    spark-submit --master spark://spark-master:7077 --deploy-mode client \
        -m etl_app_spark --job bronze --table users --date 2026-05-01
"""
from __future__ import annotations

import argparse
import uuid

from etl_app_spark.jobs.bronze import AuthBronzeJob
from etl_app_spark.jobs.gold import AuthGoldJob
from etl_app_spark.jobs.silver import AuthSilverJob
from etl_app_spark.session import SparkSessionFactory

_JOBS = {
    "bronze": AuthBronzeJob,
    "silver": AuthSilverJob,
    "gold": AuthGoldJob,
}


def parse_args():
    p = argparse.ArgumentParser(description="Auth medallion PySpark job runner")
    p.add_argument("--job", required=True, choices=list(_JOBS), help="Medallion layer")
    p.add_argument("--table", required=True, help="Table/dim/fact name within the layer")
    p.add_argument("--date", required=True, help="Logical date (YYYY-MM-DD)")
    p.add_argument("--run-id", default=None, help="Orchestration run id (default: random uuid)")
    return p.parse_args()


def main():
    args = parse_args()
    run_id = args.run_id or f"manual-{uuid.uuid4()}"
    spark = SparkSessionFactory.build(f"auth_{args.job}_{args.table}_{args.date}")
    try:
        job = _JOBS[args.job](spark, args.table, args.date, run_id)
        job.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
