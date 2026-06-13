"""Bronze job — Postgres `auth` tables → auth_bronze.*_raw (as-is + meta)."""
from __future__ import annotations

from pyspark.sql import DataFrame

from etl_app_spark import catalog, config, meta
from etl_app_spark.jobs.base import SparkJob
from etl_app_spark.schemas.source_tables import SPECS


class AuthBronzeJob(SparkJob):
    layer = "bronze"

    def __init__(self, spark, table, logical_date, run_id):
        super().__init__(spark, table, logical_date, run_id)
        if table not in SPECS:
            raise SystemExit(f"unknown bronze table '{table}' (have: {', '.join(SPECS)})")
        self.spec = SPECS[table]

    def extract(self) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", config.jdbc_url())
            .option("dbtable", self.spec.select_sql(self.logical_date))
            .option("user", config.PG_USER)
            .option("password", config.PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return meta.add_bronze_meta(df, self.logical_date, self.run_id)

    def load(self, df: DataFrame) -> None:
        fqn = catalog.table_fqn("bronze", self.spec.bronze)
        write_mode = "snapshot" if self.spec.mode == "snapshot" else "append"
        catalog.write_iceberg(df, fqn, partition_cols=["logical_date"], mode=write_mode)
