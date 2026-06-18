
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

BRONZE_TABLE = (BRONZE_NS, "hackernews_raw")
SILVER_TABLE = (SILVER_NS, "hackernews_stories")
GOLD_TABLE = (GOLD_NS, "hackernews_daily")


BRONZE_SCHEMA = Schema(
    NestedField(1, "story_id", IntegerType(), required=True),
    NestedField(2, "title", StringType(), required=True),
    NestedField(3, "author", StringType(), required=False),
    NestedField(4, "url", StringType(), required=False),
    NestedField(5, "points", IntegerType(), required=False),
    NestedField(6, "num_comments", IntegerType(), required=False),
    NestedField(7, "created_at", StringType(), required=False),
    NestedField(8, "story_type", StringType(), required=False),
    NestedField(9, "ingest_ts", TimestamptzType(), required=True),
    NestedField(10, "raw_payload", StringType(), required=False),
)

BRONZE_PARTITION = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="ingest_day"),
)


SILVER_SCHEMA = Schema(
    NestedField(1, "story_id", IntegerType(), required=True),
    NestedField(2, "title", StringType(), required=True),
    NestedField(3, "author", StringType(), required=False),
    NestedField(4, "url", StringType(), required=False),
    NestedField(5, "points", IntegerType(), required=False),
    NestedField(6, "num_comments", IntegerType(), required=False),
    NestedField(7, "created_at", TimestamptzType(), required=False),
    NestedField(8, "story_type", StringType(), required=False),
    
)

SILVER_PARTITION = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="story_day"),
)


GOLD_SCHEMA = Schema(
    NestedField(1, "day", DateType(), required=True),
    NestedField(2, "total_stories", IntegerType(), required=True),
    NestedField(3, "avg_points", DoubleType(), required=False),
    NestedField(4, "avg_comments", DoubleType(), required=False),
    NestedField(5, "author", StringType(), required=True),
    NestedField(6, "computed_at", TimestamptzType(), required=False),
)

GOLD_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="day"),
)
