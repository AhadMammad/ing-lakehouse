"""silver.airquality_hourly — typed + deduped per (sensor_id, hour_utc).

Reads bronze rows for --date, deduplicates on (sensor_id, hour_utc) keeping
the most recent ingest, drops hours with no data (percent_coverage == 0),
derives measurement_date and hour_of_day, then upserts into silver.
Re-runs are idempotent.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.airquality.schemas import (
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

    # Select only the columns present in bronze that map to silver
    # (measurement_date and hour_of_day are derived below)
    bronze_cols = tuple(
        f.name for f in SILVER_SCHEMA.fields
        if f.name not in ("measurement_date", "hour_of_day")
    )
    bronze_rows = bronze.scan(
        row_filter=EqualTo("logical_date", logical_date.isoformat()),
        selected_fields=bronze_cols,
    ).to_arrow()

    df = pl.from_arrow(bronze_rows)
    if df.is_empty():
        print(f"[airquality_silver] no bronze rows for {date}")
        return

    # Keep latest ingest per (sensor_id, hour_utc); API may re-publish corrections.
    df = (
        df.sort("ingest_ts", descending=True)
        .unique(subset=["sensor_id", "hour_utc"], keep="first")
    )

    # Drop hours with zero coverage (sensor was offline the entire hour).
    df = df.filter(
        pl.col("percent_coverage").is_not_null() & (pl.col("percent_coverage") > 0)
    )

    if df.is_empty():
        print(f"[airquality_silver] all rows had zero coverage for {date}")
        return

    df = df.with_columns(
        pl.col("hour_utc").dt.date().alias("measurement_date"),
        pl.col("hour_utc").dt.hour().cast(pl.Int32).alias("hour_of_day"),
    ).select([f.name for f in SILVER_SCHEMA.fields])

    silver = _load_or_create_silver(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(SILVER_SCHEMA))
    result = silver.upsert(arrow, join_cols=["sensor_id", "hour_utc"])
    silver.refresh()
    print(
        f"[airquality_silver] {date}: inserted={result.rows_inserted} "
        f"updated={result.rows_updated} snapshots={len(silver.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
