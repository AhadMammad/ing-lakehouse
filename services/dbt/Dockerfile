# ─────────────────────────────────────────────────────────────────
#  dbt — Trino-targeted transforms for the Rideon medallion.
#
#  Built per-instance and tagged as ${COMPOSE_PROJECT_NAME}-dbt:latest
#  (see services/dbt/docker-compose.yml). Airflow's rideon_etl DAG spawns
#  ephemeral containers from this image via DockerOperator to build the
#  silver star schema and gold marts in the Nessie iceberg catalog.
#
#  Profiles are baked into the image and templated from env vars at run
#  time (the Airflow worker can't bind-mount — the docker-socket-proxy
#  denies VOLUMES). ENTRYPOINT is `dbt`, so callers pass dbt subcommands:
#     command="build --select path:models/silver --target prod"
# ─────────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir \
      "dbt-core==1.9.*" \
      "dbt-trino==1.9.*"

COPY . /usr/app/dbt

# dbt reads profiles.yml from here; project files live in the same dir.
ENV DBT_PROFILES_DIR=/usr/app/dbt

ENTRYPOINT ["dbt"]
