"""Postgres source reader for the payments medallion.

`pl.read_database_uri` with `connectorx` engine is the lightest path:
no driver to compile, fast columnar reads, and Decimal/UUID handled
without manual casts. Partition-key date filters are inlined into SQL
(not bound) because connectorx does not support parameter binding.
"""
from __future__ import annotations

import polars as pl

from etl_app.config import get_postgres_uri

SOURCE_NAME = "postgres-source"


def read_sql(sql: str) -> pl.DataFrame:
    return pl.read_database_uri(query=sql, uri=get_postgres_uri(), engine="connectorx")
