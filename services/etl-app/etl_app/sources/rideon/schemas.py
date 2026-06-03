"""Iceberg bronze schemas + SQL templates for the Rideon medallion.

Bronze (10 tables) lands raw OLTP from the `rideon` source database —
UUIDs as text, money/geo as Decimal, timestamps preserved, plus the
standard housekeeping columns (`ingest_ts`, `logical_date`, `source`,
`raw_payload`). Silver (star schema) and gold (marts) for Rideon are
built by dbt-on-Trino, not here.

Mirrors the structure of sources/postgres/schemas.py so jobs/rideon_bronze.py
can reuse the same enrich/coerce/load flow as payments_bronze.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

BRONZE_NS = "bronze"

# ─────────────────────────────────────────────────────────────────────
#  Source columns per bronze table (drive raw_payload JSON + field order).
# ─────────────────────────────────────────────────────────────────────
BRONZE_SOURCE_COLS: dict[str, list[str]] = {
    "cities": ["city_id", "name", "country", "timezone", "launched_at"],
    "vehicle_categories": [
        "category_id", "code", "display_name",
        "base_fare", "per_km_rate", "per_min_rate", "min_fare",
    ],
    "riders": [
        "rider_id", "first_name", "last_name", "email",
        "phone", "city_id", "rating", "created_at",
    ],
    "drivers": [
        "driver_id", "first_name", "last_name", "email", "phone",
        "city_id", "license_number", "status", "rating", "onboarded_at",
    ],
    "vehicles": [
        "vehicle_id", "driver_id", "category_id", "make", "model",
        "year", "plate_number", "color", "registered_at",
    ],
    "rides": [
        "ride_id", "rider_id", "driver_id", "vehicle_id", "city_id",
        "category_id", "status", "requested_at", "accepted_at",
        "started_at", "completed_at", "pickup_lat", "pickup_lng",
        "dropoff_lat", "dropoff_lng", "distance_km", "duration_min",
        "surge_multiplier", "cancelled_by", "created_at", "updated_at",
    ],
    "fares": [
        "fare_id", "ride_id", "base_fare", "distance_fare", "time_fare",
        "surge_amount", "discount", "total_fare", "currency", "created_at",
    ],
    "ride_payments": [
        "payment_id", "ride_id", "rider_id", "method", "amount",
        "currency", "status", "created_at", "updated_at",
    ],
    "ratings": [
        "rating_id", "ride_id", "rater_role", "score", "comment", "created_at",
    ],
    "driver_payouts": [
        "payout_id", "driver_id", "payout_date", "gross_amount",
        "commission", "net_amount", "currency", "rides_count",
        "status", "created_at",
    ],
}

# Per-table SQL builders. UUID -> text keeps connectorx from returning
# python uuid.UUID. The date is validated upstream so f-string interp is safe.
SOURCE_SQL: dict[str, Callable[[str], str]] = {
    "cities": lambda _d: (
        "SELECT city_id::text, name, country, timezone, launched_at FROM cities"
    ),
    "vehicle_categories": lambda _d: (
        "SELECT category_id::text, code, display_name, base_fare, per_km_rate, "
        "per_min_rate, min_fare FROM vehicle_categories"
    ),
    "riders": lambda _d: (
        "SELECT rider_id::text, first_name, last_name, email, phone, "
        "city_id::text, rating, created_at FROM riders"
    ),
    "drivers": lambda _d: (
        "SELECT driver_id::text, first_name, last_name, email, phone, "
        "city_id::text, license_number, status, rating, onboarded_at FROM drivers"
    ),
    "vehicles": lambda _d: (
        "SELECT vehicle_id::text, driver_id::text, category_id::text, make, "
        "model, year, plate_number, color, registered_at FROM vehicles"
    ),
    "rides": lambda d: (
        "SELECT ride_id::text, rider_id::text, driver_id::text, vehicle_id::text, "
        "city_id::text, category_id::text, status, requested_at, accepted_at, "
        "started_at, completed_at, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, "
        "distance_km, duration_min, surge_multiplier, cancelled_by, created_at, updated_at "
        f"FROM rides WHERE updated_at::date = DATE '{d}'"
    ),
    "fares": lambda d: (
        "SELECT fare_id::text, ride_id::text, base_fare, distance_fare, time_fare, "
        "surge_amount, discount, total_fare, currency, created_at "
        f"FROM fares WHERE created_at::date = DATE '{d}'"
    ),
    "ride_payments": lambda d: (
        "SELECT payment_id::text, ride_id::text, rider_id::text, method, amount, "
        "currency, status, created_at, updated_at "
        f"FROM ride_payments WHERE updated_at::date = DATE '{d}'"
    ),
    "ratings": lambda d: (
        "SELECT rating_id::text, ride_id::text, rater_role, score, comment, created_at "
        f"FROM ratings WHERE created_at::date = DATE '{d}'"
    ),
    "driver_payouts": lambda d: (
        "SELECT payout_id::text, driver_id::text, payout_date, gross_amount, "
        "commission, net_amount, currency, rides_count, status, created_at "
        f"FROM driver_payouts WHERE payout_date = DATE '{d}'"
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


def _housekeeping(start_id: int) -> tuple[NestedField, ...]:
    """The four trailing housekeeping fields, with ingest_ts at `start_id`."""
    return (
        NestedField(start_id, "ingest_ts", TimestamptzType(), required=True),
        NestedField(start_id + 1, "logical_date", DateType(), required=True),
        NestedField(start_id + 2, "source", StringType(), required=True),
        NestedField(start_id + 3, "raw_payload", StringType(), required=False),
    )


_CITIES_SCHEMA = Schema(
    NestedField(1, "city_id", StringType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
    NestedField(4, "timezone", StringType(), required=False),
    NestedField(5, "launched_at", TimestamptzType(), required=False),
    *_housekeeping(6),
)

_VEHICLE_CATEGORIES_SCHEMA = Schema(
    NestedField(1, "category_id", StringType(), required=True),
    NestedField(2, "code", StringType(), required=False),
    NestedField(3, "display_name", StringType(), required=False),
    NestedField(4, "base_fare", DecimalType(8, 2), required=False),
    NestedField(5, "per_km_rate", DecimalType(8, 2), required=False),
    NestedField(6, "per_min_rate", DecimalType(8, 2), required=False),
    NestedField(7, "min_fare", DecimalType(8, 2), required=False),
    *_housekeeping(8),
)

_RIDERS_SCHEMA = Schema(
    NestedField(1, "rider_id", StringType(), required=True),
    NestedField(2, "first_name", StringType(), required=False),
    NestedField(3, "last_name", StringType(), required=False),
    NestedField(4, "email", StringType(), required=False),
    NestedField(5, "phone", StringType(), required=False),
    NestedField(6, "city_id", StringType(), required=False),
    NestedField(7, "rating", DecimalType(2, 1), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    *_housekeeping(9),
)

_DRIVERS_SCHEMA = Schema(
    NestedField(1, "driver_id", StringType(), required=True),
    NestedField(2, "first_name", StringType(), required=False),
    NestedField(3, "last_name", StringType(), required=False),
    NestedField(4, "email", StringType(), required=False),
    NestedField(5, "phone", StringType(), required=False),
    NestedField(6, "city_id", StringType(), required=False),
    NestedField(7, "license_number", StringType(), required=False),
    NestedField(8, "status", StringType(), required=False),
    NestedField(9, "rating", DecimalType(2, 1), required=False),
    NestedField(10, "onboarded_at", TimestamptzType(), required=False),
    *_housekeeping(11),
)

_VEHICLES_SCHEMA = Schema(
    NestedField(1, "vehicle_id", StringType(), required=True),
    NestedField(2, "driver_id", StringType(), required=False),
    NestedField(3, "category_id", StringType(), required=False),
    NestedField(4, "make", StringType(), required=False),
    NestedField(5, "model", StringType(), required=False),
    NestedField(6, "year", IntegerType(), required=False),  # PG SMALLINT -> Int16 -> coerced Int32
    NestedField(7, "plate_number", StringType(), required=False),
    NestedField(8, "color", StringType(), required=False),
    NestedField(9, "registered_at", TimestamptzType(), required=False),
    *_housekeeping(10),
)

_RIDES_SCHEMA = Schema(
    NestedField(1, "ride_id", StringType(), required=True),
    NestedField(2, "rider_id", StringType(), required=False),
    NestedField(3, "driver_id", StringType(), required=False),
    NestedField(4, "vehicle_id", StringType(), required=False),
    NestedField(5, "city_id", StringType(), required=False),
    NestedField(6, "category_id", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "requested_at", TimestamptzType(), required=False),
    NestedField(9, "accepted_at", TimestamptzType(), required=False),
    NestedField(10, "started_at", TimestamptzType(), required=False),
    NestedField(11, "completed_at", TimestamptzType(), required=False),
    NestedField(12, "pickup_lat", DecimalType(9, 6), required=False),
    NestedField(13, "pickup_lng", DecimalType(9, 6), required=False),
    NestedField(14, "dropoff_lat", DecimalType(9, 6), required=False),
    NestedField(15, "dropoff_lng", DecimalType(9, 6), required=False),
    NestedField(16, "distance_km", DecimalType(8, 2), required=False),
    NestedField(17, "duration_min", DecimalType(8, 2), required=False),
    NestedField(18, "surge_multiplier", DecimalType(4, 2), required=False),
    NestedField(19, "cancelled_by", StringType(), required=False),
    NestedField(20, "created_at", TimestamptzType(), required=False),
    NestedField(21, "updated_at", TimestamptzType(), required=False),
    *_housekeeping(22),
)

_FARES_SCHEMA = Schema(
    NestedField(1, "fare_id", StringType(), required=True),
    NestedField(2, "ride_id", StringType(), required=False),
    NestedField(3, "base_fare", DecimalType(10, 2), required=False),
    NestedField(4, "distance_fare", DecimalType(10, 2), required=False),
    NestedField(5, "time_fare", DecimalType(10, 2), required=False),
    NestedField(6, "surge_amount", DecimalType(10, 2), required=False),
    NestedField(7, "discount", DecimalType(10, 2), required=False),
    NestedField(8, "total_fare", DecimalType(10, 2), required=False),
    NestedField(9, "currency", StringType(), required=False),
    NestedField(10, "created_at", TimestamptzType(), required=False),
    *_housekeeping(11),
)

_RIDE_PAYMENTS_SCHEMA = Schema(
    NestedField(1, "payment_id", StringType(), required=True),
    NestedField(2, "ride_id", StringType(), required=False),
    NestedField(3, "rider_id", StringType(), required=False),
    NestedField(4, "method", StringType(), required=False),
    NestedField(5, "amount", DecimalType(10, 2), required=False),
    NestedField(6, "currency", StringType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "created_at", TimestamptzType(), required=False),
    NestedField(9, "updated_at", TimestamptzType(), required=False),
    *_housekeeping(10),
)

_RATINGS_SCHEMA = Schema(
    NestedField(1, "rating_id", StringType(), required=True),
    NestedField(2, "ride_id", StringType(), required=False),
    NestedField(3, "rater_role", StringType(), required=False),
    NestedField(4, "score", IntegerType(), required=False),  # PG SMALLINT -> coerced Int32
    NestedField(5, "comment", StringType(), required=False),
    NestedField(6, "created_at", TimestamptzType(), required=False),
    *_housekeeping(7),
)

_DRIVER_PAYOUTS_SCHEMA = Schema(
    NestedField(1, "payout_id", StringType(), required=True),
    NestedField(2, "driver_id", StringType(), required=False),
    NestedField(3, "payout_date", DateType(), required=False),
    NestedField(4, "gross_amount", DecimalType(12, 2), required=False),
    NestedField(5, "commission", DecimalType(12, 2), required=False),
    NestedField(6, "net_amount", DecimalType(12, 2), required=False),
    NestedField(7, "currency", StringType(), required=False),
    NestedField(8, "rides_count", IntegerType(), required=False),
    NestedField(9, "status", StringType(), required=False),
    NestedField(10, "created_at", TimestamptzType(), required=False),
    *_housekeeping(11),
)


BRONZE_SPECS: dict[str, BronzeSpec] = {
    "cities": BronzeSpec(
        (BRONZE_NS, "rideon_cities_raw"), _CITIES_SCHEMA,
        _bronze_partition(6), "snapshot", SOURCE_SQL["cities"],
        BRONZE_SOURCE_COLS["cities"],
    ),
    "vehicle_categories": BronzeSpec(
        (BRONZE_NS, "rideon_vehicle_categories_raw"), _VEHICLE_CATEGORIES_SCHEMA,
        _bronze_partition(8), "snapshot", SOURCE_SQL["vehicle_categories"],
        BRONZE_SOURCE_COLS["vehicle_categories"],
    ),
    "riders": BronzeSpec(
        (BRONZE_NS, "rideon_riders_raw"), _RIDERS_SCHEMA,
        _bronze_partition(9), "snapshot", SOURCE_SQL["riders"],
        BRONZE_SOURCE_COLS["riders"],
    ),
    "drivers": BronzeSpec(
        (BRONZE_NS, "rideon_drivers_raw"), _DRIVERS_SCHEMA,
        _bronze_partition(11), "snapshot", SOURCE_SQL["drivers"],
        BRONZE_SOURCE_COLS["drivers"],
    ),
    "vehicles": BronzeSpec(
        (BRONZE_NS, "rideon_vehicles_raw"), _VEHICLES_SCHEMA,
        _bronze_partition(10), "snapshot", SOURCE_SQL["vehicles"],
        BRONZE_SOURCE_COLS["vehicles"],
    ),
    "rides": BronzeSpec(
        (BRONZE_NS, "rideon_rides_raw"), _RIDES_SCHEMA,
        _bronze_partition(22), "incremental", SOURCE_SQL["rides"],
        BRONZE_SOURCE_COLS["rides"],
    ),
    "fares": BronzeSpec(
        (BRONZE_NS, "rideon_fares_raw"), _FARES_SCHEMA,
        _bronze_partition(11), "incremental", SOURCE_SQL["fares"],
        BRONZE_SOURCE_COLS["fares"],
    ),
    "ride_payments": BronzeSpec(
        (BRONZE_NS, "rideon_ride_payments_raw"), _RIDE_PAYMENTS_SCHEMA,
        _bronze_partition(10), "incremental", SOURCE_SQL["ride_payments"],
        BRONZE_SOURCE_COLS["ride_payments"],
    ),
    "ratings": BronzeSpec(
        (BRONZE_NS, "rideon_ratings_raw"), _RATINGS_SCHEMA,
        _bronze_partition(7), "incremental", SOURCE_SQL["ratings"],
        BRONZE_SOURCE_COLS["ratings"],
    ),
    "driver_payouts": BronzeSpec(
        (BRONZE_NS, "rideon_driver_payouts_raw"), _DRIVER_PAYOUTS_SCHEMA,
        _bronze_partition(11), "incremental", SOURCE_SQL["driver_payouts"],
        BRONZE_SOURCE_COLS["driver_payouts"],
    ),
}
