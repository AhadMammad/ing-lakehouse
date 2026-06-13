"""Best-practice medallion meta-column helpers.

Bronze meta mirrors services/etl-app (ingest_ts, logical_date, source,
raw_payload) plus provenance additions (_ingest_run_id, _source_system).
Silver/gold add load + change-detection columns.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_app_spark.config import SOURCE_SYSTEM


def add_bronze_meta(df: DataFrame, logical_date: str, run_id: str) -> DataFrame:
    """Append bronze meta columns. raw_payload = JSON of the source columns."""
    source_cols = df.columns
    return (
        df.withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in source_cols])))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("logical_date", F.to_date(F.lit(logical_date)))
        .withColumn("source", F.lit(SOURCE_SYSTEM))
        .withColumn("_ingest_run_id", F.lit(run_id))
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
    )


def add_silver_meta(df: DataFrame, business_cols: list[str], logical_date: str) -> DataFrame:
    """Append silver meta columns (load time, record hash, source logical date)."""
    return (
        df.withColumn(
            "_record_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]), 256),
        )
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_source_logical_date", F.to_date(F.lit(logical_date)))
    )


def record_hash(cols: list[str]):
    """Spark Column expression: sha2-256 over the given columns (NULL-safe)."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)
