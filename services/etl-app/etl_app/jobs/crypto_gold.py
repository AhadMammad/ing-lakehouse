"""gold.crypto_market_daily — daily market summary with top movers.

Reads silver rows for `--date`, computes one summary row, overwrites the
slice for that day.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.crypto.schemas import (
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
    day = dt.date.fromisoformat(date)
    catalog = get_catalog()

    silver = catalog.load_table(SILVER_TABLE)
    silver.refresh()
    rows = silver.scan(row_filter=EqualTo("day", date)).to_arrow()

    df = pl.from_arrow(rows)
    if df.is_empty():
        print(f"[crypto_gold] no silver rows for {date}")
        return

    # Filter out coins missing the 24h change before picking movers.
    movers = df.filter(pl.col("price_change_percentage_24h").is_not_null())
    if movers.is_empty():
        top_gainer = (None, None)
        top_loser = (None, None)
    else:
        top_gainer_row = movers.sort("price_change_percentage_24h", descending=True).row(0, named=True)
        top_loser_row = movers.sort("price_change_percentage_24h", descending=False).row(0, named=True)
        top_gainer = (top_gainer_row["coin_id"], top_gainer_row["price_change_percentage_24h"])
        top_loser = (top_loser_row["coin_id"], top_loser_row["price_change_percentage_24h"])

    volume = df.filter(pl.col("total_volume_usd").is_not_null())
    if volume.is_empty():
        top_vol = (None, None)
    else:
        top_vol_row = volume.sort("total_volume_usd", descending=True).row(0, named=True)
        top_vol = (top_vol_row["coin_id"], top_vol_row["total_volume_usd"])

    summary = pl.DataFrame(
        [
            {
                "day": day,
                "coin_count": df.height,
                "total_market_cap_usd": df["market_cap_usd"].sum(),
                "total_volume_usd": df["total_volume_usd"].sum(),
                "top_gainer_coin_id": top_gainer[0],
                "top_gainer_pct_24h": top_gainer[1],
                "top_loser_coin_id": top_loser[0],
                "top_loser_pct_24h": top_loser[1],
                "highest_volume_coin_id": top_vol[0],
                "highest_volume_usd": top_vol[1],
                "computed_at": dt.datetime.now(tz=UTC),
            }
        ],
        schema_overrides={"coin_count": pl.Int32},
    ).select([f.name for f in GOLD_SCHEMA.fields])

    gold = _load_or_create_gold(catalog)
    arrow = summary.to_arrow().cast(schema_to_pyarrow(GOLD_SCHEMA))
    gold.overwrite(arrow, overwrite_filter=EqualTo("day", date))
    gold.refresh()
    print(f"[crypto_gold] {date}: wrote 1 summary row; snapshots={len(gold.history())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    main(parser.parse_args().date)
