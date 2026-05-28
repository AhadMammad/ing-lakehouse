"""Iceberg schemas + SQL templates for the payments medallion.

Bronze (7 tables) lands raw OLTP — UUIDs as text, amounts as Decimal(14,2),
timestamps preserved, plus housekeeping columns (`ingest_ts`, `logical_date`,
`source`, `raw_payload`).

Silver (8 tables) is a Kimball star: dim_customer / dim_merchant /
dim_payment_method / dim_date and fact_payments / fact_refunds /
fact_fees / fact_settlements.

Gold (4 marts) overwrites per logical day except `customer_lifetime_value`
which is a full-table recompute.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

BRONZE_NS = "bronze"
SILVER_NS = "silver"
GOLD_NS = "gold"

# ─────────────────────────────────────────────────────────────────────
#  Bronze — one Iceberg table per source OLTP table
# ─────────────────────────────────────────────────────────────────────

# Source columns per bronze table (used to build raw_payload JSON and
# to drive the field order of the schema). Order matches column IDs.
BRONZE_SOURCE_COLS: dict[str, list[str]] = {
    "merchants": [
        "merchant_id", "name", "category_code", "mcc_code",
        "country", "city", "status", "created_at",
    ],
    "customers": [
        "customer_id", "first_name", "last_name", "email", "birthdate",
        "phone", "country", "city", "created_at",
    ],
    "methods": [
        "method_id", "customer_id", "type", "provider", "card_brand",
        "last4", "expiry_month", "expiry_year", "is_default", "created_at",
    ],
    "payments": [
        "payment_id", "merchant_id", "customer_id", "method_id",
        "amount", "currency", "status", "error_code", "description",
        "reference_id", "created_at", "updated_at", "settled_at",
    ],
    "refunds": [
        "refund_id", "payment_id", "amount", "currency",
        "reason", "status", "created_at", "updated_at",
    ],
    "fees": [
        "fee_id", "payment_id", "fee_type", "amount", "currency", "created_at",
    ],
    "settlements": [
        "settlement_id", "merchant_id", "settlement_date", "amount",
        "currency", "payments_count", "status", "created_at",
    ],
}

# Per-table SQL builders.  Postgres UUID -> text cast keeps connectorx
# from returning python uuid.UUID (mixes badly with arrow).  The date is
# already validated via dt.date.fromisoformat upstream so f-string interp
# is safe.
SOURCE_SQL: dict[str, Callable[[str], str]] = {
    "merchants": lambda _d: (
        "SELECT merchant_id::text, name, category_code, mcc_code, country, "
        "city, status, created_at FROM merchants"
    ),
    "customers": lambda _d: (
        "SELECT customer_id::text, first_name, last_name, email, birthdate, "
        "phone, country, city, created_at FROM customers"
    ),
    "methods": lambda _d: (
        "SELECT method_id::text, customer_id::text, type, provider, card_brand, "
        "last4, expiry_month, expiry_year, is_default, created_at "
        "FROM payment_methods"
    ),
    "payments": lambda d: (
        "SELECT payment_id::text, merchant_id::text, customer_id::text, "
        "method_id::text, amount, currency, status, error_code, description, "
        "reference_id, created_at, updated_at, settled_at "
        f"FROM payments WHERE updated_at::date = DATE '{d}'"
    ),
    "refunds": lambda d: (
        "SELECT refund_id::text, payment_id::text, amount, currency, "
        "reason, status, created_at, updated_at "
        f"FROM refunds WHERE updated_at::date = DATE '{d}'"
    ),
    "fees": lambda d: (
        "SELECT fee_id::text, payment_id::text, fee_type, amount, currency, "
        f"created_at FROM fees WHERE created_at::date = DATE '{d}'"
    ),
    "settlements": lambda d: (
        "SELECT settlement_id::text, merchant_id::text, settlement_date, "
        "amount, currency, payments_count, status, created_at "
        f"FROM settlements WHERE settlement_date = DATE '{d}'"
    ),
}


@dataclass(frozen=True)
class BronzeSpec:
    identifier: tuple[str, str]
    schema: Schema
    partition_spec: PartitionSpec
    mode: str  # "snapshot" | "incremental"
    sql: Callable[[str], str]
    source_cols: list[str]


def _bronze_partition(ingest_ts_field_id: int) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=ingest_ts_field_id,
            field_id=1000,
            transform=DayTransform(),
            name="ingest_day",
        ),
    )


_MERCHANTS_SCHEMA = Schema(
    NestedField(1, "merchant_id", StringType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "category_code", StringType(), required=False),
    NestedField(4, "mcc_code", StringType(), required=False),
    NestedField(5, "country", StringType(), required=False),
    NestedField(6, "city", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    NestedField(9, "ingest_ts", TimestamptzType(), required=True),
    NestedField(10, "logical_date", DateType(), required=True),
    NestedField(11, "source", StringType(), required=True),
    NestedField(12, "raw_payload", StringType(), required=False),
)

_CUSTOMERS_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=True),
    NestedField(2, "first_name", StringType(), required=False),
    NestedField(3, "last_name", StringType(), required=False),
    NestedField(4, "email", StringType(), required=False),
    NestedField(5, "birthdate", DateType(), required=False),
    NestedField(6, "phone", StringType(), required=False),
    NestedField(7, "country", StringType(), required=False),
    NestedField(8, "city", StringType(), required=False),
    NestedField(9, "created_at", TimestamptzType(), required=False),
    NestedField(10, "ingest_ts", TimestamptzType(), required=True),
    NestedField(11, "logical_date", DateType(), required=True),
    NestedField(12, "source", StringType(), required=True),
    NestedField(13, "raw_payload", StringType(), required=False),
)

_METHODS_SCHEMA = Schema(
    NestedField(1, "method_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "type", StringType(), required=False),
    NestedField(4, "provider", StringType(), required=False),
    NestedField(5, "card_brand", StringType(), required=False),
    NestedField(6, "last4", StringType(), required=False),
    NestedField(7, "expiry_month", IntegerType(), required=False),
    NestedField(8, "expiry_year", IntegerType(), required=False),
    NestedField(9, "is_default", BooleanType(), required=False),
    NestedField(10, "created_at", TimestamptzType(), required=False),
    NestedField(11, "ingest_ts", TimestamptzType(), required=True),
    NestedField(12, "logical_date", DateType(), required=True),
    NestedField(13, "source", StringType(), required=True),
    NestedField(14, "raw_payload", StringType(), required=False),
)

_PAYMENTS_SCHEMA = Schema(
    NestedField(1, "payment_id", StringType(), required=True),
    NestedField(2, "merchant_id", StringType(), required=False),
    NestedField(3, "customer_id", StringType(), required=False),
    NestedField(4, "method_id", StringType(), required=False),
    NestedField(5, "amount", DecimalType(14, 2), required=False),
    NestedField(6, "currency", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "error_code", StringType(), required=False),
    NestedField(9, "description", StringType(), required=False),
    NestedField(10, "reference_id", StringType(), required=False),
    NestedField(11, "created_at", TimestamptzType(), required=False),
    NestedField(12, "updated_at", TimestamptzType(), required=False),
    NestedField(13, "settled_at", TimestamptzType(), required=False),
    NestedField(14, "ingest_ts", TimestamptzType(), required=True),
    NestedField(15, "logical_date", DateType(), required=True),
    NestedField(16, "source", StringType(), required=True),
    NestedField(17, "raw_payload", StringType(), required=False),
)

_REFUNDS_SCHEMA = Schema(
    NestedField(1, "refund_id", StringType(), required=True),
    NestedField(2, "payment_id", StringType(), required=False),
    NestedField(3, "amount", DecimalType(14, 2), required=False),
    NestedField(4, "currency", StringType(), required=False),
    NestedField(5, "reason", StringType(), required=False),
    NestedField(6, "status", StringType(), required=False),
    NestedField(7, "created_at", TimestamptzType(), required=False),
    NestedField(8, "updated_at", TimestamptzType(), required=False),
    NestedField(9, "ingest_ts", TimestamptzType(), required=True),
    NestedField(10, "logical_date", DateType(), required=True),
    NestedField(11, "source", StringType(), required=True),
    NestedField(12, "raw_payload", StringType(), required=False),
)

_FEES_SCHEMA = Schema(
    NestedField(1, "fee_id", StringType(), required=True),
    NestedField(2, "payment_id", StringType(), required=False),
    NestedField(3, "fee_type", StringType(), required=False),
    NestedField(4, "amount", DecimalType(14, 2), required=False),
    NestedField(5, "currency", StringType(), required=False),
    NestedField(6, "created_at", TimestamptzType(), required=False),
    NestedField(7, "ingest_ts", TimestamptzType(), required=True),
    NestedField(8, "logical_date", DateType(), required=True),
    NestedField(9, "source", StringType(), required=True),
    NestedField(10, "raw_payload", StringType(), required=False),
)

_SETTLEMENTS_SCHEMA = Schema(
    NestedField(1, "settlement_id", StringType(), required=True),
    NestedField(2, "merchant_id", StringType(), required=False),
    NestedField(3, "settlement_date", DateType(), required=False),
    NestedField(4, "amount", DecimalType(14, 2), required=False),
    NestedField(5, "currency", StringType(), required=False),
    NestedField(6, "payments_count", IntegerType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    NestedField(9, "ingest_ts", TimestamptzType(), required=True),
    NestedField(10, "logical_date", DateType(), required=True),
    NestedField(11, "source", StringType(), required=True),
    NestedField(12, "raw_payload", StringType(), required=False),
)

# Bronze partition: DayTransform on ingest_ts (always the last source-data
# column + 1 in our layout — captured per-schema).
BRONZE_SPECS: dict[str, BronzeSpec] = {
    "merchants": BronzeSpec(
        (BRONZE_NS, "payments_merchants_raw"), _MERCHANTS_SCHEMA,
        _bronze_partition(9), "snapshot", SOURCE_SQL["merchants"],
        BRONZE_SOURCE_COLS["merchants"],
    ),
    "customers": BronzeSpec(
        (BRONZE_NS, "payments_customers_raw"), _CUSTOMERS_SCHEMA,
        _bronze_partition(10), "snapshot", SOURCE_SQL["customers"],
        BRONZE_SOURCE_COLS["customers"],
    ),
    "methods": BronzeSpec(
        (BRONZE_NS, "payments_methods_raw"), _METHODS_SCHEMA,
        _bronze_partition(11), "snapshot", SOURCE_SQL["methods"],
        BRONZE_SOURCE_COLS["methods"],
    ),
    "payments": BronzeSpec(
        (BRONZE_NS, "payments_payments_raw"), _PAYMENTS_SCHEMA,
        _bronze_partition(14), "incremental", SOURCE_SQL["payments"],
        BRONZE_SOURCE_COLS["payments"],
    ),
    "refunds": BronzeSpec(
        (BRONZE_NS, "payments_refunds_raw"), _REFUNDS_SCHEMA,
        _bronze_partition(9), "incremental", SOURCE_SQL["refunds"],
        BRONZE_SOURCE_COLS["refunds"],
    ),
    "fees": BronzeSpec(
        (BRONZE_NS, "payments_fees_raw"), _FEES_SCHEMA,
        _bronze_partition(7), "incremental", SOURCE_SQL["fees"],
        BRONZE_SOURCE_COLS["fees"],
    ),
    "settlements": BronzeSpec(
        (BRONZE_NS, "payments_settlements_raw"), _SETTLEMENTS_SCHEMA,
        _bronze_partition(9), "incremental", SOURCE_SQL["settlements"],
        BRONZE_SOURCE_COLS["settlements"],
    ),
}


# ─────────────────────────────────────────────────────────────────────
#  Silver — Kimball star schema
# ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SilverSpec:
    identifier: tuple[str, str]
    schema: Schema
    partition_spec: PartitionSpec | None  # None → unpartitioned (small dims)
    join_cols: list[str]                  # PK(s) for upsert
    bronze_key: str                       # which BRONZE_SPECS entry feeds this
    incremental: bool                     # True → filter bronze by logical_date


_DIM_CUSTOMER_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=True),
    NestedField(2, "first_name", StringType(), required=False),
    NestedField(3, "last_name", StringType(), required=False),
    NestedField(4, "email", StringType(), required=False),
    NestedField(5, "birthdate", DateType(), required=False),
    NestedField(6, "age_band", StringType(), required=False),
    NestedField(7, "country", StringType(), required=False),
    NestedField(8, "city", StringType(), required=False),
    NestedField(9, "phone", StringType(), required=False),
    NestedField(10, "created_at", TimestamptzType(), required=False),
    NestedField(11, "updated_at", TimestamptzType(), required=True),
)

_DIM_MERCHANT_SCHEMA = Schema(
    NestedField(1, "merchant_id", StringType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "category_code", StringType(), required=False),
    NestedField(4, "mcc_code", StringType(), required=False),
    NestedField(5, "country", StringType(), required=False),
    NestedField(6, "city", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    NestedField(9, "updated_at", TimestamptzType(), required=True),
)

_DIM_PAYMENT_METHOD_SCHEMA = Schema(
    NestedField(1, "method_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "type", StringType(), required=False),
    NestedField(4, "provider", StringType(), required=False),
    NestedField(5, "card_brand", StringType(), required=False),
    NestedField(6, "last4", StringType(), required=False),
    NestedField(7, "expiry_month", IntegerType(), required=False),
    NestedField(8, "expiry_year", IntegerType(), required=False),
    NestedField(9, "is_default", BooleanType(), required=False),
    NestedField(10, "is_expired", BooleanType(), required=False),
    NestedField(11, "created_at", TimestamptzType(), required=False),
    NestedField(12, "updated_at", TimestamptzType(), required=True),
)

_DIM_DATE_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=True),
    NestedField(2, "day", IntegerType(), required=True),
    NestedField(3, "month", IntegerType(), required=True),
    NestedField(4, "quarter", IntegerType(), required=True),
    NestedField(5, "year", IntegerType(), required=True),
    NestedField(6, "day_of_week", IntegerType(), required=True),
    NestedField(7, "day_name", StringType(), required=True),
    NestedField(8, "is_weekend", BooleanType(), required=True),
    NestedField(9, "iso_week", IntegerType(), required=True),
)

_FACT_PAYMENTS_SCHEMA = Schema(
    NestedField(1, "payment_id", StringType(), required=True),
    NestedField(2, "payment_date", DateType(), required=True),
    NestedField(3, "customer_id", StringType(), required=False),
    NestedField(4, "merchant_id", StringType(), required=False),
    NestedField(5, "method_id", StringType(), required=False),
    NestedField(6, "amount", DecimalType(14, 2), required=False),
    NestedField(7, "currency", StringType(), required=False),
    NestedField(8, "status", StringType(), required=False),
    NestedField(9, "error_code", StringType(), required=False),
    NestedField(10, "description", StringType(), required=False),
    NestedField(11, "reference_id", StringType(), required=False),
    NestedField(12, "created_at", TimestamptzType(), required=False),
    NestedField(13, "updated_at", TimestamptzType(), required=False),
    NestedField(14, "settled_at", TimestamptzType(), required=False),
    NestedField(15, "settled_lag_hours", DoubleType(), required=False),
)

_FACT_REFUNDS_SCHEMA = Schema(
    NestedField(1, "refund_id", StringType(), required=True),
    NestedField(2, "refund_date", DateType(), required=True),
    NestedField(3, "payment_id", StringType(), required=False),
    NestedField(4, "amount", DecimalType(14, 2), required=False),
    NestedField(5, "currency", StringType(), required=False),
    NestedField(6, "reason", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    NestedField(9, "updated_at", TimestamptzType(), required=False),
)

_FACT_FEES_SCHEMA = Schema(
    NestedField(1, "fee_id", StringType(), required=True),
    NestedField(2, "fee_date", DateType(), required=True),
    NestedField(3, "payment_id", StringType(), required=False),
    NestedField(4, "fee_type", StringType(), required=False),
    NestedField(5, "amount", DecimalType(14, 2), required=False),
    NestedField(6, "currency", StringType(), required=False),
    NestedField(7, "created_at", TimestamptzType(), required=False),
)

_FACT_SETTLEMENTS_SCHEMA = Schema(
    NestedField(1, "settlement_id", StringType(), required=True),
    NestedField(2, "settlement_date", DateType(), required=True),
    NestedField(3, "merchant_id", StringType(), required=False),
    NestedField(4, "amount", DecimalType(14, 2), required=False),
    NestedField(5, "currency", StringType(), required=False),
    NestedField(6, "payments_count", IntegerType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
)


def _identity_partition(source_id: int, name: str) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000,
                       transform=IdentityTransform(), name=name),
    )


SILVER_SPECS: dict[str, SilverSpec] = {
    "dim_customer": SilverSpec(
        (SILVER_NS, "dim_customer"), _DIM_CUSTOMER_SCHEMA, None,
        ["customer_id"], "customers", incremental=False,
    ),
    "dim_merchant": SilverSpec(
        (SILVER_NS, "dim_merchant"), _DIM_MERCHANT_SCHEMA, None,
        ["merchant_id"], "merchants", incremental=False,
    ),
    "dim_payment_method": SilverSpec(
        (SILVER_NS, "dim_payment_method"), _DIM_PAYMENT_METHOD_SCHEMA, None,
        ["method_id"], "methods", incremental=False,
    ),
    # dim_date is built deterministically from --date, no bronze source.
    "dim_date": SilverSpec(
        (SILVER_NS, "dim_date"), _DIM_DATE_SCHEMA, None,
        ["date"], bronze_key="", incremental=False,
    ),
    "fact_payments": SilverSpec(
        (SILVER_NS, "fact_payments"), _FACT_PAYMENTS_SCHEMA,
        _identity_partition(2, "payment_date"),
        ["payment_id"], "payments", incremental=True,
    ),
    "fact_refunds": SilverSpec(
        (SILVER_NS, "fact_refunds"), _FACT_REFUNDS_SCHEMA,
        _identity_partition(2, "refund_date"),
        ["refund_id"], "refunds", incremental=True,
    ),
    "fact_fees": SilverSpec(
        (SILVER_NS, "fact_fees"), _FACT_FEES_SCHEMA,
        _identity_partition(2, "fee_date"),
        ["fee_id"], "fees", incremental=True,
    ),
    "fact_settlements": SilverSpec(
        (SILVER_NS, "fact_settlements"), _FACT_SETTLEMENTS_SCHEMA,
        _identity_partition(2, "settlement_date"),
        ["settlement_id"], "settlements", incremental=True,
    ),
}


# ─────────────────────────────────────────────────────────────────────
#  Gold — aggregate marts
# ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GoldSpec:
    identifier: tuple[str, str]
    schema: Schema
    partition_spec: PartitionSpec | None
    overwrite_column: str | None  # None → full-table overwrite


_GOLD_DAILY_PAYMENT_SUMMARY = Schema(
    NestedField(1, "day", DateType(), required=True),
    NestedField(2, "total_payments", IntegerType(), required=True),
    NestedField(3, "total_volume", DecimalType(14, 2), required=False),
    NestedField(4, "successful_count", IntegerType(), required=True),
    NestedField(5, "failed_count", IntegerType(), required=True),
    NestedField(6, "success_rate_pct", DoubleType(), required=False),
    NestedField(7, "avg_amount", DecimalType(14, 2), required=False),
    NestedField(8, "unique_customers", IntegerType(), required=True),
    NestedField(9, "unique_merchants", IntegerType(), required=True),
    NestedField(10, "refund_volume", DecimalType(14, 2), required=False),
    NestedField(11, "fee_volume", DecimalType(14, 2), required=False),
    NestedField(12, "net_revenue", DecimalType(14, 2), required=False),
    NestedField(13, "computed_at", TimestamptzType(), required=True),
)

_GOLD_DAILY_REVENUE_BY_MERCHANT = Schema(
    NestedField(1, "merchant_id", StringType(), required=True),
    NestedField(2, "merchant_name", StringType(), required=False),
    NestedField(3, "category_code", StringType(), required=False),
    NestedField(4, "day", DateType(), required=True),
    NestedField(5, "gross_volume", DecimalType(14, 2), required=False),
    NestedField(6, "txn_count", IntegerType(), required=True),
    NestedField(7, "success_count", IntegerType(), required=True),
    NestedField(8, "refund_amount", DecimalType(14, 2), required=False),
    NestedField(9, "fee_amount", DecimalType(14, 2), required=False),
    NestedField(10, "net_revenue", DecimalType(14, 2), required=False),
    NestedField(11, "avg_ticket", DecimalType(14, 2), required=False),
    NestedField(12, "computed_at", TimestamptzType(), required=True),
)

_GOLD_CLV = Schema(
    NestedField(1, "customer_id", StringType(), required=True),
    NestedField(2, "full_name", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
    NestedField(4, "age_band", StringType(), required=False),
    NestedField(5, "total_spend", DecimalType(14, 2), required=False),
    NestedField(6, "total_payments", IntegerType(), required=True),
    NestedField(7, "successful_payments", IntegerType(), required=True),
    NestedField(8, "failed_payments", IntegerType(), required=True),
    NestedField(9, "total_refunds", DecimalType(14, 2), required=False),
    NestedField(10, "refund_rate_pct", DoubleType(), required=False),
    NestedField(11, "first_payment_date", DateType(), required=False),
    NestedField(12, "last_payment_date", DateType(), required=False),
    NestedField(13, "active_days", IntegerType(), required=True),
    NestedField(14, "computed_at", TimestamptzType(), required=True),
)

_GOLD_MERCHANT_SETTLEMENT_DAILY = Schema(
    NestedField(1, "merchant_id", StringType(), required=True),
    NestedField(2, "merchant_name", StringType(), required=False),
    NestedField(3, "settlement_date", DateType(), required=True),
    NestedField(4, "settled_amount", DecimalType(14, 2), required=False),
    NestedField(5, "settled_payments_count", IntegerType(), required=True),
    NestedField(6, "gross_volume_for_date", DecimalType(14, 2), required=False),
    NestedField(7, "settlement_lag_days", IntegerType(), required=False),
    NestedField(8, "status", StringType(), required=False),
    NestedField(9, "computed_at", TimestamptzType(), required=True),
)


GOLD_SPECS: dict[str, GoldSpec] = {
    "daily_payment_summary": GoldSpec(
        (GOLD_NS, "daily_payment_summary"), _GOLD_DAILY_PAYMENT_SUMMARY,
        _identity_partition(1, "day"), "day",
    ),
    "daily_revenue_by_merchant": GoldSpec(
        (GOLD_NS, "daily_revenue_by_merchant"), _GOLD_DAILY_REVENUE_BY_MERCHANT,
        _identity_partition(4, "day"), "day",
    ),
    "customer_lifetime_value": GoldSpec(
        (GOLD_NS, "customer_lifetime_value"), _GOLD_CLV,
        None, None,  # full-table overwrite
    ),
    "merchant_settlement_daily": GoldSpec(
        (GOLD_NS, "merchant_settlement_daily"), _GOLD_MERCHANT_SETTLEMENT_DAILY,
        _identity_partition(3, "settlement_date"), "settlement_date",
    ),
}
