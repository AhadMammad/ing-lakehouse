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
#
# The auth_etl DAG takes approach (c): a BashOperator runs
# `docker exec <spark-master> spark-submit ...` against the docker-socket-proxy
# (DOCKER_HOST=tcp://docker-socket-proxy:2375, which permits EXEC + POST). That
# only needs the docker CLI *client* on PATH (no daemon, no JDK). Install it as
# root, then drop back to the airflow user.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && chmod a+r /etc/apt/keyrings/docker.asc \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" \
      > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends docker-ce-cli \
 && rm -rf /var/lib/apt/lists/*
USER airflow
