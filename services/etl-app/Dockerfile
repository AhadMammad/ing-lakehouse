# ─────────────────────────────────────────────────────────────────
#  etl-app — Lightweight Python image for Iceberg ETL jobs.
#
#  Built per-instance and tagged as ${COMPOSE_PROJECT_NAME}-etl-app:latest
#  (see services/etl-app/docker-compose.yml). Airflow's DAG spawns
#  ephemeral containers from this image via DockerOperator.
#
#  Entrypoint is `python -m`, so DAG passes module paths as the command:
#     command="etl_app.jobs.bronze --date 2026-05-21"
# ─────────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

# Pin versions to match the Jupyter notebook environment so behavior
# stays consistent between exploration and production runs.
RUN pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow]==0.11.1" \
      "polars==1.9.0" \
      "requests==2.32.3" \
      "connectorx==0.3.3"

COPY etl_app /app/etl_app

ENTRYPOINT ["python", "-m"]
