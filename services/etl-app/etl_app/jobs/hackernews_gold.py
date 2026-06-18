

from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.hackernews.schemas import (
    GOLD_NS,
    GOLD_PARTITION,
    GOLD_SCHEMA,
    GOLD_TABLE,
    SILVER_TABLE,
)

UTC = dt.timezone.utc


def _day_bounds(date: str) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.fromisoformat(date).replace(tzinfo=UTC)
    return start, start + dt.timedelta(days=1)


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
    start, end = _day_bounds(date)
    day = start.date()
    catalog = get_catalog()

    silver = catalog.load_table(SILVER_TABLE)
    silver.refresh()
    rows = silver.scan(
        row_filter=And(
            GreaterThanOrEqual("created_at", start.isoformat()),
            LessThan("created_at", end.isoformat()),
        ),
    ).to_arrow()

    df = pl.from_arrow(rows)
    if df.is_empty():
        print(f"[hackernews_gold] no silver rows for {date}")
        return

    agg = (
        df.group_by("author")
        .agg(
            pl.col("story_id").count().alias("total_stories"),
            pl.col("points").mean().alias("avg_points"),
            pl.col("num_comments").mean().alias("avg_comments"),
            
        )
        .with_columns(
            pl.lit(day).alias("day"),
            pl.lit(dt.datetime.now(tz=UTC)).alias("computed_at"),
        )
        .select([f.name for f in GOLD_SCHEMA.fields])
    )

    gold = _load_or_create_gold(catalog)
    arrow = agg.to_arrow().cast(schema_to_pyarrow(GOLD_SCHEMA))
    gold.overwrite(
        arrow,
        overwrite_filter=EqualTo("day", day.isoformat()),
    )
    gold.refresh()
    print(f"[hackernews_gold] {date}: wrote {len(agg)} aggregate row(s); snapshots={len(gold.history())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
