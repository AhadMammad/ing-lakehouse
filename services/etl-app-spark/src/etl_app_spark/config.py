"""Runtime configuration read from environment variables.

Mirrors the env contract of services/etl-app/etl_app/config.py so the same
Airflow / container wiring populates both apps. Module-level constants — no
Settings class, no Pydantic.

The Spark services receive these via `env_file: ../../.env` (RUSTFS_*/AWS_*)
and the spark-submit `docker exec` inherits the container env; PG_* are the
postgres-source connection for the JDBC bronze read.
"""
from __future__ import annotations

import os

# ── Iceberg / Nessie / RustFS ─────────────────────────────────────
CATALOG_NAME = "nessie"
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://rustfs:9000")
# RUSTFS_* are the canonical names in .env; AWS_* are the S3 SDK aliases.
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ["RUSTFS_ACCESS_KEY"]
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ["RUSTFS_SECRET_KEY"]
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
WAREHOUSE_BUCKET = os.environ.get("ICEBERG_WAREHOUSE_BUCKET", "iceberg-warehouse")
WAREHOUSE_URI = f"s3://{WAREHOUSE_BUCKET}/warehouse"

# ── Provenance ────────────────────────────────────────────────────
SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "postgres.auth")

# ── Postgres source (auth DB) ─────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "postgres-source")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "auth")
PG_USER = os.environ.get("PG_USER", "payments")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "payments123")


def jdbc_url() -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def jdbc_properties() -> dict[str, str]:
    return {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
