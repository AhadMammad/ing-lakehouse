"""Runtime configuration read from environment variables.

DockerOperator injects these at spawn time from the Airflow worker's
own env (which Compose populates from the active .env.<instance>).
Module-level constants — no Settings class, no Pydantic.
"""
from __future__ import annotations

import os

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://rustfs:9000")
S3_KEY = os.environ["AWS_ACCESS_KEY_ID"]
S3_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
WAREHOUSE_BUCKET = os.environ.get("ICEBERG_WAREHOUSE_BUCKET", "iceberg-warehouse")
WAREHOUSE_URI = f"s3://{WAREHOUSE_BUCKET}/warehouse"


def _pg_uri() -> str:
    host = os.environ["PG_HOST"]
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ["PG_DB"]
    user = os.environ["PG_USER"]
    pw = os.environ["PG_PASSWORD"]
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def get_postgres_uri() -> str:
    return _pg_uri()
