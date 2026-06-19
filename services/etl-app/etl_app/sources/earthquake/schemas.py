"""Iceberg table schemas + partition specs for the earthquake medallion.

bronze.earthquake_raw    — raw USGS GeoJSON rows, append-only, partitioned by ingest day
silver.earthquake_events — typed + deduped by event_id, partitioned by event day
gold.earthquake_daily    — per-day global aggregations, overwritten by event_date
"""
from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
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

BRONZE_TABLE = (BRONZE_NS, "earthquake_raw")
SILVER_TABLE = (SILVER_NS, "earthquake_events")
GOLD_TABLE = (GOLD_NS, "earthquake_daily")


BRONZE_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "mag", DoubleType(), required=False),
    NestedField(3, "mag_type", StringType(), required=False),
    NestedField(4, "place", StringType(), required=False),
    NestedField(5, "event_time", TimestamptzType(), required=True),
    NestedField(6, "updated_time", TimestamptzType(), required=False),
    NestedField(7, "latitude", DoubleType(), required=False),
    NestedField(8, "longitude", DoubleType(), required=False),
    NestedField(9, "depth_km", DoubleType(), required=False),
    NestedField(10, "alert", StringType(), required=False),
    NestedField(11, "tsunami", IntegerType(), required=False),
    NestedField(12, "sig", IntegerType(), required=False),
    NestedField(13, "net", StringType(), required=False),
    NestedField(14, "status", StringType(), required=False),
    NestedField(15, "ingest_ts", TimestamptzType(), required=True),
    NestedField(16, "logical_date", DateType(), required=True),
    NestedField(17, "source", StringType(), required=True),
    NestedField(18, "raw_payload", StringType(), required=False),
)

BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=15, field_id=1000, transform=DayTransform(), name="ingest_day"),
)


SILVER_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "mag", DoubleType(), required=False),
    NestedField(3, "mag_type", StringType(), required=False),
    NestedField(4, "place", StringType(), required=False),
    NestedField(5, "event_time", TimestamptzType(), required=True),
    NestedField(6, "updated_time", TimestamptzType(), required=False),
    NestedField(7, "latitude", DoubleType(), required=False),
    NestedField(8, "longitude", DoubleType(), required=False),
    NestedField(9, "depth_km", DoubleType(), required=False),
    NestedField(10, "alert", StringType(), required=False),
    NestedField(11, "tsunami", IntegerType(), required=False),
    NestedField(12, "sig", IntegerType(), required=False),
    NestedField(13, "net", StringType(), required=False),
    NestedField(14, "status", StringType(), required=False),
    NestedField(15, "event_date", DateType(), required=True),
    NestedField(16, "magnitude_class", StringType(), required=False),
)

SILVER_PARTITION = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="event_day"),
)


GOLD_SCHEMA = Schema(
    NestedField(1, "event_date", DateType(), required=True),
    NestedField(2, "total_count", IntegerType(), required=True),
    NestedField(3, "max_magnitude", DoubleType(), required=False),
    NestedField(4, "avg_magnitude", DoubleType(), required=False),
    NestedField(5, "avg_depth_km", DoubleType(), required=False),
    NestedField(6, "tsunami_count", IntegerType(), required=True),
    NestedField(7, "significant_count", IntegerType(), required=True),
    NestedField(8, "micro_count", IntegerType(), required=True),
    NestedField(9, "minor_count", IntegerType(), required=True),
    NestedField(10, "light_count", IntegerType(), required=True),
    NestedField(11, "moderate_count", IntegerType(), required=True),
    NestedField(12, "strong_count", IntegerType(), required=True),
    NestedField(13, "major_count", IntegerType(), required=True),
    NestedField(14, "great_count", IntegerType(), required=True),
    NestedField(15, "most_significant_place", StringType(), required=False),
    NestedField(16, "most_significant_magnitude", DoubleType(), required=False),
)

GOLD_PARTITION = PartitionSpec()
