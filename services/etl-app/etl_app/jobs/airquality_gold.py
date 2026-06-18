"""gold.airquality_daily — daily aggregates per (location, parameter).

Reads silver rows for --date, groups by (location_id, parameter_name, day),
computes avg/min/max and hourly reading count, adds WHO 2021 AQI category for
PM2.5, then overwrites the measurement_date slice in gold. Atomic and idempotent.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.airquality.schemas import (
    GOLD_NS,
    GOLD_PARTITION,
    GOLD_SCHEMA,
    GOLD_TABLE,
    SILVER_TABLE,
)


def _aqi_category(avg_pm25: float | None) -> str | None:
    """WHO 2021 PM2.5 daily mean thresholds (µg/m³)."""
    if avg_pm25 is None:
        return None
    if avg_pm25 <= 5:
        return "good"
    if avg_pm25 <= 15:
        return "moderate"
    if avg_pm25 <= 25:
        return "unhealthy_sensitive"
    if avg_pm25 <= 35:
        return "unhealthy"
    if avg_pm25 <= 45:
        return "very_unhealthy"
    return "hazardous"


def _load_or_create_gold(catalog):
    if catalog.table_exists(GOLD_TABLE):
        return catalog.load_table(GOLD_TABLE)
    ensure_namespace(catalog, GOLD_NS)
    return catalog.create_table(
        identifier=GOLD_TABLE,
        schema=GOLD_SCHEMA,
        partition_spec=GOLD_PARTITION,
    )


def main(date: str) -> None:
    measurement_date = dt.date.fromisoformat(date)
    catalog = get_catalog()

    if not catalog.table_exists(SILVER_TABLE):
        print(f"[airquality_gold] silver table does not exist yet — skipping {date}")
        return
    silver = catalog.load_table(SILVER_TABLE)
    silver.refresh()
    rows = silver.scan(
        row_filter=EqualTo("measurement_date", measurement_date.isoformat()),
    ).to_arrow()

    df = pl.from_arrow(rows)
    if df.is_empty():
        print(f"[airquality_gold] no silver rows for {date}")
        return

    agg = (
        df.group_by(["location_id", "location_name", "country_code", "parameter_name", "measurement_date"])
        .agg(
            pl.col("value").mean().alias("avg_value"),
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
            pl.col("value").count().cast(pl.Int32).alias("hourly_reading_count"),
        )
    )

    agg = agg.with_columns(
        pl.when(pl.col("parameter_name") == "pm25")
        .then(
            pl.col("avg_value").map_elements(_aqi_category, return_dtype=pl.String)
        )
        .otherwise(None)
        .alias("aqi_category")
    ).select([f.name for f in GOLD_SCHEMA.fields])

    gold = _load_or_create_gold(catalog)
    arrow = agg.to_arrow().cast(schema_to_pyarrow(GOLD_SCHEMA))
    gold.overwrite(
        arrow,
        overwrite_filter=EqualTo("measurement_date", measurement_date.isoformat()),
    )
    gold.refresh()
    print(
        f"[airquality_gold] {date}: wrote {len(agg)} aggregate row(s); "
        f"snapshots={len(gold.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
