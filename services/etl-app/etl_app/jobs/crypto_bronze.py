"""bronze.crypto_prices_raw — snapshot top-N CoinGecko coins and append.

Append-only. `logical_date` carries the run's `--date`; `ingest_ts` is now().
Each coin is stored as its own row with the per-coin JSON kept in
`raw_payload` for replay.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.crypto.client import SOURCE_NAME, fetch_top_markets
from etl_app.sources.crypto.schemas import (
    BRONZE_NS,
    BRONZE_PARTITION,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
)


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    # CoinGecko uses 'Z' suffix; fromisoformat handles it on 3.11+.
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _flatten(
    coin: dict,
    ingest_ts: dt.datetime,
    logical_date: dt.date,
) -> dict:
    return {
        "coin_id": coin["id"],
        "symbol": coin.get("symbol"),
        "name": coin.get("name"),
        "current_price_usd": coin.get("current_price"),
        "market_cap_usd": coin.get("market_cap"),
        "market_cap_rank": coin.get("market_cap_rank"),
        "total_volume_usd": coin.get("total_volume"),
        "high_24h_usd": coin.get("high_24h"),
        "low_24h_usd": coin.get("low_24h"),
        "price_change_24h_usd": coin.get("price_change_24h"),
        "price_change_percentage_24h": coin.get("price_change_percentage_24h_in_currency"),
        "price_change_percentage_7d": coin.get("price_change_percentage_7d_in_currency"),
        "market_cap_change_24h": coin.get("market_cap_change_24h"),
        "market_cap_change_percentage_24h": coin.get("market_cap_change_percentage_24h"),
        "circulating_supply": coin.get("circulating_supply"),
        "total_supply": coin.get("total_supply"),
        "max_supply": coin.get("max_supply"),
        "ath_usd": coin.get("ath"),
        "ath_change_percentage": coin.get("ath_change_percentage"),
        "last_updated": _parse_iso(coin.get("last_updated")),
        "ingest_ts": ingest_ts,
        "logical_date": logical_date,
        "source": SOURCE_NAME,
        "raw_payload": json.dumps(coin, separators=(",", ":")),
    }


def _load_or_create(catalog):
    if catalog.table_exists(BRONZE_TABLE):
        return catalog.load_table(BRONZE_TABLE)
    ensure_namespace(catalog, BRONZE_NS)
    return catalog.create_table(
        identifier=BRONZE_TABLE,
        schema=BRONZE_SCHEMA,
        partition_spec=BRONZE_PARTITION,
    )


def main(date: str) -> None:
    logical_date = dt.date.fromisoformat(date)
    ingest_ts = dt.datetime.now(tz=dt.timezone.utc)
    coins = fetch_top_markets()
    rows = [_flatten(c, ingest_ts, logical_date) for c in coins]
    if not rows:
        print(f"[crypto_bronze] no coins returned for {date}")
        return

    df = pl.DataFrame(rows)
    catalog = get_catalog()
    table = _load_or_create(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(BRONZE_SCHEMA))
    table.append(arrow)
    table.refresh()
    print(f"[crypto_bronze] appended {len(rows)} rows for {date}; snapshots={len(table.history())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    main(parser.parse_args().date)
