"""gold.earthquake_daily — global daily aggregates over silver.earthquake_events.

Reads silver rows for `--date`, computes one summary row for the day, and
overwrites the event_date slice in gold. Atomic and idempotent.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.earthquake.schemas import (
    GOLD_NS,
    GOLD_PARTITION,
    GOLD_SCHEMA,
    GOLD_TABLE,
    SILVER_TABLE,
)

UTC = dt.timezone.utc


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
    event_date = dt.date.fromisoformat(date)
    catalog = get_catalog()

    silver = catalog.load_table(SILVER_TABLE)
    silver.refresh()
    rows = silver.scan(
        row_filter=EqualTo("event_date", event_date.isoformat()),
    ).to_arrow()

    df = pl.from_arrow(rows)
    if df.is_empty():
        print(f"[earthquake_gold] no silver rows for {date}")
        return

    # Row with highest significance score for most_significant_place/magnitude.
    top = df.sort("sig", descending=True, nulls_last=True).row(0, named=True)

    agg = df.select(
        pl.lit(event_date).alias("event_date"),
        pl.len().cast(pl.Int32).alias("total_count"),
        pl.col("mag").max().alias("max_magnitude"),
        pl.col("mag").mean().alias("avg_magnitude"),
        pl.col("depth_km").mean().alias("avg_depth_km"),
        pl.col("tsunami").fill_null(0).cast(pl.Int32).sum().alias("tsunami_count"),
        (pl.col("sig").fill_null(0) >= 600).cast(pl.Int32).sum().alias("significant_count"),
        (pl.col("mag") < 2.0).cast(pl.Int32).sum().alias("micro_count"),
        ((pl.col("mag") >= 2.0) & (pl.col("mag") < 4.0)).cast(pl.Int32).sum().alias("minor_count"),
        ((pl.col("mag") >= 4.0) & (pl.col("mag") < 5.0)).cast(pl.Int32).sum().alias("light_count"),
        ((pl.col("mag") >= 5.0) & (pl.col("mag") < 6.0)).cast(pl.Int32).sum().alias("moderate_count"),
        ((pl.col("mag") >= 6.0) & (pl.col("mag") < 7.0)).cast(pl.Int32).sum().alias("strong_count"),
        ((pl.col("mag") >= 7.0) & (pl.col("mag") < 8.0)).cast(pl.Int32).sum().alias("major_count"),
        (pl.col("mag") >= 8.0).cast(pl.Int32).sum().alias("great_count"),
        pl.lit(top.get("place")).alias("most_significant_place"),
        pl.lit(top.get("mag")).alias("most_significant_magnitude"),
    )

    gold = _load_or_create_gold(catalog)
    arrow = agg.to_arrow().cast(schema_to_pyarrow(GOLD_SCHEMA))
    gold.overwrite(
        arrow,
        overwrite_filter=EqualTo("event_date", event_date.isoformat()),
    )
    gold.refresh()
    print(
        f"[earthquake_gold] {date}: wrote {len(agg)} aggregate row(s); "
        f"snapshots={len(gold.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
