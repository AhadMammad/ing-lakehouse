"""silver.crypto_prices_daily — one row per (coin_id, day), upserted.

Reads bronze rows where `logical_date == --date`, dedupes per coin keeping
the latest `ingest_ts`, drops bronze-only columns, and upserts into silver.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.crypto.schemas import (
    BRONZE_TABLE,
    SILVER_NS,
    SILVER_PARTITION,
    SILVER_SCHEMA,
    SILVER_TABLE,
)


def _load_or_create_silver(catalog):
    if catalog.table_exists(SILVER_TABLE):
        return catalog.load_table(SILVER_TABLE)
    ensure_namespace(catalog, SILVER_NS)
    return catalog.create_table(
        identifier=SILVER_TABLE,
        schema=SILVER_SCHEMA,
        partition_spec=SILVER_PARTITION,
    )


def main(date: str) -> None:
    logical_date = dt.date.fromisoformat(date)
    catalog = get_catalog()

    bronze = catalog.load_table(BRONZE_TABLE)
    bronze.refresh()
    bronze_rows = bronze.scan(
        row_filter=EqualTo("logical_date", date),
    ).to_arrow()

    df = pl.from_arrow(bronze_rows)
    if df.is_empty():
        print(f"[crypto_silver] no bronze rows for {date}")
        return

    # Latest ingest_ts wins per coin for this day.
    df = (
        df.sort("ingest_ts", descending=True)
        .unique(subset=["coin_id", "logical_date"], keep="first")
        .with_columns(pl.lit(logical_date).alias("day"))
        .select([f.name for f in SILVER_SCHEMA.fields])
    )

    silver = _load_or_create_silver(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(SILVER_SCHEMA))
    result = silver.upsert(arrow, join_cols=["coin_id", "day"])
    silver.refresh()
    print(
        f"[crypto_silver] {date}: inserted={result.rows_inserted} "
        f"updated={result.rows_updated} snapshots={len(silver.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    main(parser.parse_args().date)
