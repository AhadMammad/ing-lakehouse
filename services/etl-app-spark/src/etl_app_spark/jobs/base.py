"""SparkJob abstract base — the extract → transform → load template."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_app_spark import catalog


class SparkJob(ABC):
    """One medallion job for a single (layer, table, logical_date)."""

    layer: str = ""

    def __init__(self, spark: SparkSession, table: str, logical_date: str, run_id: str):
        self.spark = spark
        self.table = table
        self.logical_date = logical_date
        self.run_id = run_id

    # ── Template method ──
    def run(self) -> None:
        catalog.ensure_namespace(self.spark, self.layer)
        df = self.extract()
        out = self.transform(df)
        self.load(out)
        self.log_done(out)

    @abstractmethod
    def extract(self) -> DataFrame: ...

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame: ...

    @abstractmethod
    def load(self, df: DataFrame) -> None: ...

    def log_done(self, df: DataFrame) -> None:
        print(
            f"[{self.layer}:{self.table}] date={self.logical_date} "
            f"run_id={self.run_id} rows={df.count()}"
        )
