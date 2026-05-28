# ─────────────────────────────────────────────────────────────────
#  data-generator — Faker-based payments data seeder.
#
#  Built per-instance and tagged as
#  ${COMPOSE_PROJECT_NAME}-data-generator:latest.
#  Airflow's payments_seeder DAG and `make run-data-generator` spawn
#  ephemeral containers from this image via DockerOperator.
#
#  Entrypoint is `python -m`, so callers pass the module + CLI args:
#    command="data_generator --start-date 2024-01-01 --end-date 2024-01-31 --rows-per-day 1000"
# ─────────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      "faker==26.0.0" \
      "psycopg2-binary==2.9.9"

COPY data_generator /app/data_generator

ENTRYPOINT ["python", "-m"]
