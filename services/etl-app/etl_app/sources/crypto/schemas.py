"""Iceberg schemas for the CoinGecko crypto medallion.

bronze.crypto_prices_raw   — per-coin snapshot, append-only, day-partitioned on ingest
silver.crypto_prices_daily — one row per (coin_id, day), upserted, day-partitioned
gold.crypto_market_daily   — one row per day with market totals + top movers
"""
from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

BRONZE_NS = "bronze"
SILVER_NS = "silver"
GOLD_NS = "gold"

BRONZE_TABLE = (BRONZE_NS, "crypto_prices_raw")
SILVER_TABLE = (SILVER_NS, "crypto_prices_daily")
GOLD_TABLE = (GOLD_NS, "crypto_market_daily")


BRONZE_SCHEMA = Schema(
    NestedField(1, "coin_id", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=False),
    NestedField(3, "name", StringType(), required=False),
    NestedField(4, "current_price_usd", DoubleType(), required=False),
    NestedField(5, "market_cap_usd", DoubleType(), required=False),
    NestedField(6, "market_cap_rank", IntegerType(), required=False),
    NestedField(7, "total_volume_usd", DoubleType(), required=False),
    NestedField(8, "high_24h_usd", DoubleType(), required=False),
    NestedField(9, "low_24h_usd", DoubleType(), required=False),
    NestedField(10, "price_change_24h_usd", DoubleType(), required=False),
    NestedField(11, "price_change_percentage_24h", DoubleType(), required=False),
    NestedField(12, "price_change_percentage_7d", DoubleType(), required=False),
    NestedField(13, "market_cap_change_24h", DoubleType(), required=False),
    NestedField(14, "market_cap_change_percentage_24h", DoubleType(), required=False),
    NestedField(15, "circulating_supply", DoubleType(), required=False),
    NestedField(16, "total_supply", DoubleType(), required=False),
    NestedField(17, "max_supply", DoubleType(), required=False),
    NestedField(18, "ath_usd", DoubleType(), required=False),
    NestedField(19, "ath_change_percentage", DoubleType(), required=False),
    NestedField(20, "last_updated", TimestamptzType(), required=False),
    NestedField(21, "ingest_ts", TimestamptzType(), required=True),
    NestedField(22, "logical_date", DateType(), required=True),
    NestedField(23, "source", StringType(), required=True),
    NestedField(24, "raw_payload", StringType(), required=False),
)

BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=21, field_id=1000, transform=DayTransform(), name="ingest_day"),
)


SILVER_SCHEMA = Schema(
    NestedField(1, "coin_id", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=False),
    NestedField(3, "name", StringType(), required=False),
    NestedField(4, "day", DateType(), required=True),
    NestedField(5, "current_price_usd", DoubleType(), required=False),
    NestedField(6, "market_cap_usd", DoubleType(), required=False),
    NestedField(7, "market_cap_rank", IntegerType(), required=False),
    NestedField(8, "total_volume_usd", DoubleType(), required=False),
    NestedField(9, "high_24h_usd", DoubleType(), required=False),
    NestedField(10, "low_24h_usd", DoubleType(), required=False),
    NestedField(11, "price_change_24h_usd", DoubleType(), required=False),
    NestedField(12, "price_change_percentage_24h", DoubleType(), required=False),
    NestedField(13, "price_change_percentage_7d", DoubleType(), required=False),
    NestedField(14, "market_cap_change_24h", DoubleType(), required=False),
    NestedField(15, "market_cap_change_percentage_24h", DoubleType(), required=False),
    NestedField(16, "circulating_supply", DoubleType(), required=False),
    NestedField(17, "total_supply", DoubleType(), required=False),
    NestedField(18, "max_supply", DoubleType(), required=False),
    NestedField(19, "ath_usd", DoubleType(), required=False),
    NestedField(20, "ath_change_percentage", DoubleType(), required=False),
    NestedField(21, "last_updated", TimestamptzType(), required=False),
    NestedField(22, "source", StringType(), required=True),
)

SILVER_PARTITION = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="day"),
)


GOLD_SCHEMA = Schema(
    NestedField(1, "day", DateType(), required=True),
    NestedField(2, "coin_count", IntegerType(), required=True),
    NestedField(3, "total_market_cap_usd", DoubleType(), required=False),
    NestedField(4, "total_volume_usd", DoubleType(), required=False),
    NestedField(5, "top_gainer_coin_id", StringType(), required=False),
    NestedField(6, "top_gainer_pct_24h", DoubleType(), required=False),
    NestedField(7, "top_loser_coin_id", StringType(), required=False),
    NestedField(8, "top_loser_pct_24h", DoubleType(), required=False),
    NestedField(9, "highest_volume_coin_id", StringType(), required=False),
    NestedField(10, "highest_volume_usd", DoubleType(), required=False),
    NestedField(11, "computed_at", TimestamptzType(), required=True),
)

GOLD_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="day"),
)
