ARG AIRFLOW_VERSION=3.2.1
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.13

# Add the providers the docs assume. The base apache/airflow:3.2.1 image
# does not ship apache-spark or trino — every SparkSubmitOperator /
# TrinoHook / SQLExecuteQueryOperator(conn_id="trino_default") example in
# docs/airflow/ requires these to even import. The smtp provider supplies
# SmtpHook / SmtpNotifier used by the email alerting helper
# (dags/_notifiers.py) against the Mailpit smtp_default connection. The
# telegram provider supplies TelegramHook used by the Telegram alerting
# helper (dags/_telegram_notifiers.py) to post run alerts to a channel
# (this provider has no Notifier class, so the helper calls the hook).
#
# Pinning against the official constraint file keeps provider versions
# compatible with the airflow core version in the FROM tag.
RUN pip install --no-cache-dir \
      "apache-airflow-providers-apache-spark" \
      "apache-airflow-providers-trino" \
      "apache-airflow-providers-docker" \
      "apache-airflow-providers-smtp" \
      "apache-airflow-providers-telegram" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# SparkSubmitOperator requires `spark-submit` on PATH and a JRE. This image
# ships the Python provider only — Hook / Operator imports work but
# executing a Spark job needs one of:
#   (a) extending this Dockerfile with JDK 17 + Spark 3.5.3 client (~1 GB), or
#   (b) using SparkConnectHook against a Spark Connect server (not enabled
#       in this repo's services/spark/ topology today), or
#   (c) submitting via DockerOperator / BashOperator that exec's into the
#       spark-master container.
# See docs/airflow/operators-tasks.md for details and the smoke DAG at
# services/airflow/dags/lakehouse_smoke.py which deliberately exercises
# Trino + S3 only.
