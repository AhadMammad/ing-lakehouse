"""Iceberg table schemas + partition specs for the weather medallion.

bronze.weather_raw    — raw API rows, append-only, partitioned by ingest day
silver.weather_hourly — typed + deduped by (city, observation_ts), day-partitioned
gold.weather_daily    — per-city daily aggregations, partitioned by city
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

BRONZE_TABLE = (BRONZE_NS, "weather_raw")
SILVER_TABLE = (SILVER_NS, "weather_hourly")
GOLD_TABLE = (GOLD_NS, "weather_daily")


BRONZE_SCHEMA = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "lon", DoubleType(), required=False),
    NestedField(4, "observation_ts", TimestamptzType(), required=True),
    NestedField(5, "temperature_2m", DoubleType(), required=False),
    NestedField(6, "relative_humidity_2m", DoubleType(), required=False),
    NestedField(7, "wind_speed_10m", DoubleType(), required=False),
    NestedField(8, "precipitation", DoubleType(), required=False),
    NestedField(9, "pressure_msl", DoubleType(), required=False),
    NestedField(10, "cloud_cover", DoubleType(), required=False),
    NestedField(11, "ingest_ts", TimestamptzType(), required=True),
    NestedField(12, "source", StringType(), required=True),
    NestedField(13, "raw_payload", StringType(), required=False),
)

BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=11, field_id=1000, transform=DayTransform(), name="ingest_day"),
)


SILVER_SCHEMA = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "lon", DoubleType(), required=False),
    NestedField(4, "observation_ts", TimestamptzType(), required=True),
    NestedField(5, "temperature_2m", DoubleType(), required=False),
    NestedField(6, "relative_humidity_2m", DoubleType(), required=False),
    NestedField(7, "wind_speed_10m", DoubleType(), required=False),
    NestedField(8, "precipitation", DoubleType(), required=False),
    NestedField(9, "pressure_msl", DoubleType(), required=False),
    NestedField(10, "cloud_cover", DoubleType(), required=False),
    NestedField(11, "source", StringType(), required=True),
)

SILVER_PARTITION = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="observation_day"),
)


GOLD_SCHEMA = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "day", DateType(), required=True),
    NestedField(3, "temp_min", DoubleType(), required=False),
    NestedField(4, "temp_max", DoubleType(), required=False),
    NestedField(5, "temp_avg", DoubleType(), required=False),
    NestedField(6, "humidity_avg", DoubleType(), required=False),
    NestedField(7, "wind_max", DoubleType(), required=False),
    NestedField(8, "precipitation_total", DoubleType(), required=False),
    NestedField(9, "hours_observed", IntegerType(), required=True),
    NestedField(10, "computed_at", TimestamptzType(), required=True),
)

GOLD_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="city"),
)
