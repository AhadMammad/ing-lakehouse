"""Runtime configuration — read from environment variables.

DockerOperator injects these at spawn time. Module-level constants,
no Settings class, matching the style of etl_app/config.py.
"""
from __future__ import annotations

import os

PG_HOST     = os.environ.get("PG_HOST", "postgres-source")
PG_PORT     = int(os.environ.get("PG_PORT", "5432"))
PG_DB       = os.environ.get("PG_DB", "payments")
PG_USER     = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
