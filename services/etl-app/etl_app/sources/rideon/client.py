"""Postgres source reader for the Rideon medallion.

Identical mechanics to sources/postgres/client.py — connectorx via
`pl.read_database_uri`. The target database is selected purely by the
PG_DB env var (set to `rideon` by the DAG / Make target), so no separate
connection config is needed here.
"""
from __future__ import annotations

import polars as pl

from etl_app.config import get_postgres_uri

SOURCE_NAME = "rideon-source"


def read_sql(sql: str) -> pl.DataFrame:
    return pl.read_database_uri(query=sql, uri=get_postgres_uri(), engine="connectorx")
