"""silver.weather_hourly — typed + deduped per (city, observation_ts).

Reads the latest bronze rows for `--date`, drops `raw_payload`, deduplicates
on (city, observation_ts) keeping the most recent ingest, then upserts into
silver. Re-runs are idempotent.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.schemas import (
    BRONZE_TABLE,
    SILVER_NS,
    SILVER_PARTITION,
    SILVER_SCHEMA,
    SILVER_TABLE,
)

UTC = dt.timezone.utc


def _day_bounds(date: str) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.fromisoformat(date).replace(tzinfo=UTC)
    return start, start + dt.timedelta(days=1)


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
    start, end = _day_bounds(date)
    catalog = get_catalog()

    bronze = catalog.load_table(BRONZE_TABLE)
    bronze.refresh()
    bronze_rows = bronze.scan(
        row_filter=And(
            GreaterThanOrEqual("observation_ts", start.isoformat()),
            LessThan("observation_ts", end.isoformat()),
        ),
        selected_fields=tuple(f.name for f in SILVER_SCHEMA.fields) + ("ingest_ts",),
    ).to_arrow()

    df = pl.from_arrow(bronze_rows)
    if df.is_empty():
        print(f"[silver] no bronze rows for {date}")
        return

    # Keep latest ingest per (city, observation_ts); then drop ingest_ts.
    df = (
        df.sort("ingest_ts", descending=True)
        .unique(subset=["city", "observation_ts"], keep="first")
        .drop("ingest_ts")
        .select([f.name for f in SILVER_SCHEMA.fields])
    )

    silver = _load_or_create_silver(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(SILVER_SCHEMA))
    result = silver.upsert(arrow, join_cols=["city", "observation_ts"])
    silver.refresh()
    print(
        f"[silver] {date}: inserted={result.rows_inserted} "
        f"updated={result.rows_updated} snapshots={len(silver.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
