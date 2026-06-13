"""Catalog/namespace helpers for the auth medallion.

Layers map to dedicated, isolated namespaces so auth tables never collide
with the shared bronze/silver/gold used by payments/rideon:
    bronze -> auth_bronze
    silver -> auth_silver
    gold   -> auth_gold
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.dataframe import DataFrame as _DF

from etl_app_spark.config import CATALOG_NAME


def namespace(layer: str) -> str:
    return f"auth_{layer}"


def ensure_namespace(spark: SparkSession, layer: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{namespace(layer)}")


def table_fqn(layer: str, name: str) -> str:
    return f"{CATALOG_NAME}.{namespace(layer)}.{name}"


def table_exists(spark: SparkSession, fqn: str) -> bool:
    """Reliable existence check across the REST catalog (avoids 3-part Catalog API quirks)."""
    try:
        spark.table(fqn)
        return True
    except Exception:
        return False


def write_iceberg(
    df: DataFrame,
    fqn: str,
    partition_cols: list[str] | None,
    mode: str,
) -> None:
    """Create-or-write an Iceberg table.

    mode:
      "snapshot" — overwrite the partitions present in df (per-logical_date reload)
      "append"   — append rows (incremental event tables)
    On first sight the table is created from df (initial load).
    """
    from pyspark.sql.functions import col

    spark = df.sparkSession
    if not table_exists(spark, fqn):
        writer = df.writeTo(fqn).using("iceberg")
        if partition_cols:
            writer = writer.partitionedBy(col(partition_cols[0]), *[col(c) for c in partition_cols[1:]])
        writer.create()
        return
    if mode == "snapshot":
        df.writeTo(fqn).overwritePartitions()
    elif mode == "append":
        df.writeTo(fqn).append()
    else:
        raise ValueError(f"unknown write mode: {mode}")


def upsert_iceberg(df: _DF, fqn: str, keys: list[str]) -> None:
    """SCD1 upsert via Iceberg MERGE INTO on natural keys.

    First load creates the table from df; subsequent runs MERGE (update *
    / insert *). Requires the Iceberg SparkSessionExtensions (enabled in the
    session factory).
    """
    spark = df.sparkSession
    if not table_exists(spark, fqn):
        df.writeTo(fqn).using("iceberg").create()
        return
    view = "_src_" + fqn.split(".")[-1]
    df.createOrReplaceTempView(view)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(
        f"""
        MERGE INTO {fqn} t
        USING {view} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(view)
