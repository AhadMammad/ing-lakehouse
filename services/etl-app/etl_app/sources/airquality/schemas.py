"""Iceberg table schemas + partition specs for the air quality medallion.

bronze.airquality_hourly_raw — raw OpenAQ hourly measurements, append-only,
                                partitioned by ingest day
silver.airquality_hourly     — typed + deduped by (sensor_id, hour_utc),
                                partitioned by measurement day
gold.airquality_daily        — per-day aggregates per (location, parameter),
                                overwritten by measurement_date
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

BRONZE_TABLE = (BRONZE_NS, "airquality_hourly_raw")
SILVER_TABLE = (SILVER_NS, "airquality_hourly")
GOLD_TABLE = (GOLD_NS, "airquality_daily")


BRONZE_SCHEMA = Schema(
    NestedField(1, "sensor_id", IntegerType(), required=True),
    NestedField(2, "location_id", IntegerType(), required=True),
    NestedField(3, "location_name", StringType(), required=False),
    NestedField(4, "country_code", StringType(), required=False),
    NestedField(5, "latitude", DoubleType(), required=False),
    NestedField(6, "longitude", DoubleType(), required=False),
    NestedField(7, "parameter_name", StringType(), required=True),
    NestedField(8, "parameter_units", StringType(), required=False),
    NestedField(9, "parameter_id", IntegerType(), required=False),
    NestedField(10, "hour_utc", TimestamptzType(), required=True),
    NestedField(11, "value", DoubleType(), required=False),
    NestedField(12, "summary_min", DoubleType(), required=False),
    NestedField(13, "summary_max", DoubleType(), required=False),
    NestedField(14, "summary_sd", DoubleType(), required=False),
    NestedField(15, "percent_coverage", DoubleType(), required=False),
    NestedField(16, "observed_count", IntegerType(), required=False),
    NestedField(17, "ingest_ts", TimestamptzType(), required=True),
    NestedField(18, "logical_date", DateType(), required=True),
    NestedField(19, "source", StringType(), required=True),
    NestedField(20, "raw_payload", StringType(), required=False),
)

BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=17, field_id=1000, transform=DayTransform(), name="ingest_day"),
)


SILVER_SCHEMA = Schema(
    NestedField(1, "sensor_id", IntegerType(), required=True),
    NestedField(2, "location_id", IntegerType(), required=True),
    NestedField(3, "location_name", StringType(), required=False),
    NestedField(4, "country_code", StringType(), required=False),
    NestedField(5, "latitude", DoubleType(), required=False),
    NestedField(6, "longitude", DoubleType(), required=False),
    NestedField(7, "parameter_name", StringType(), required=True),
    NestedField(8, "parameter_units", StringType(), required=False),
    NestedField(9, "hour_utc", TimestamptzType(), required=True),
    NestedField(10, "measurement_date", DateType(), required=True),
    NestedField(11, "hour_of_day", IntegerType(), required=False),
    NestedField(12, "value", DoubleType(), required=False),
    NestedField(13, "summary_min", DoubleType(), required=False),
    NestedField(14, "summary_max", DoubleType(), required=False),
    NestedField(15, "percent_coverage", DoubleType(), required=False),
    NestedField(16, "ingest_ts", TimestamptzType(), required=True),
    NestedField(17, "logical_date", DateType(), required=True),
)

SILVER_PARTITION = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="measurement_day"),
)


GOLD_SCHEMA = Schema(
    NestedField(1, "location_id", IntegerType(), required=True),
    NestedField(2, "location_name", StringType(), required=False),
    NestedField(3, "country_code", StringType(), required=False),
    NestedField(4, "parameter_name", StringType(), required=True),
    NestedField(5, "measurement_date", DateType(), required=True),
    NestedField(6, "avg_value", DoubleType(), required=False),
    NestedField(7, "min_value", DoubleType(), required=False),
    NestedField(8, "max_value", DoubleType(), required=False),
    NestedField(9, "hourly_reading_count", IntegerType(), required=True),
    NestedField(10, "aqi_category", StringType(), required=False),
)

GOLD_PARTITION = PartitionSpec()
