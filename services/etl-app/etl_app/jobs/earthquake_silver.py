"""silver.earthquake_events — typed + deduped per event_id.

Reads the latest bronze rows for `--date` (by logical_date), deduplicates on
event_id keeping the most recent ingest, derives event_date and magnitude_class,
then upserts into silver. Re-runs are idempotent.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.earthquake.schemas import (
    BRONZE_TABLE,
    SILVER_NS,
    SILVER_PARTITION,
    SILVER_SCHEMA,
    SILVER_TABLE,
)

UTC = dt.timezone.utc


def _magnitude_class(mag: float | None) -> str | None:
    if mag is None:
        return None
    if mag < 2.0:
        return "micro"
    if mag < 4.0:
        return "minor"
    if mag < 5.0:
        return "light"
    if mag < 6.0:
        return "moderate"
    if mag < 7.0:
        return "strong"
    if mag < 8.0:
        return "major"
    return "great"


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
        row_filter=EqualTo("logical_date", logical_date.isoformat()),
        selected_fields=tuple(
            f.name for f in SILVER_SCHEMA.fields
            if f.name not in ("event_date", "magnitude_class")
        ) + ("ingest_ts",),
    ).to_arrow()

    df = pl.from_arrow(bronze_rows)
    if df.is_empty():
        print(f"[earthquake_silver] no bronze rows for {date}")
        return

    # Keep latest ingest per event_id (USGS updates events after initial publish).
    df = (
        df.sort("ingest_ts", descending=True)
        .unique(subset=["event_id"], keep="first")
        .drop("ingest_ts")
    )

    # Derive event_date and magnitude_class.
    df = df.with_columns(
        pl.col("event_time").dt.date().alias("event_date"),
        pl.col("mag").map_elements(_magnitude_class, return_dtype=pl.String).alias("magnitude_class"),
    ).select([f.name for f in SILVER_SCHEMA.fields])

    silver = _load_or_create_silver(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(SILVER_SCHEMA))
    result = silver.upsert(arrow, join_cols=["event_id"])
    silver.refresh()
    print(
        f"[earthquake_silver] {date}: inserted={result.rows_inserted} "
        f"updated={result.rows_updated} snapshots={len(silver.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
