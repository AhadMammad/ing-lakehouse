"""SparkSession factory wired to the Iceberg REST catalog on Nessie.

Most config is baked into /opt/spark/conf/spark-defaults.conf in the custom
image (catalog class/type/uri, s3a event-log settings). This factory only
sets the S3FileIO credentials programmatically (from env) so secrets stay out
of the baked file, mirroring the working PyIceberg config in
services/etl-app/etl_app/catalog.py.

NOTE: Nessie's Iceberg REST endpoint is unauthenticated in this stack — do
NOT set rest.auth.type / token, or catalog init fails.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from etl_app_spark import config


class SparkSessionFactory:
    @staticmethod
    def build(app_name: str) -> SparkSession:
        cat = config.CATALOG_NAME
        builder = (
            SparkSession.builder.appName(app_name)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.type", "rest")
            .config(f"spark.sql.catalog.{cat}.uri", config.NESSIE_URI)
            .config(f"spark.sql.catalog.{cat}.warehouse", config.WAREHOUSE_URI)
            .config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{cat}.s3.endpoint", config.S3_ENDPOINT)
            .config(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{cat}.s3.access-key-id", config.S3_KEY)
            .config(f"spark.sql.catalog.{cat}.s3.secret-access-key", config.S3_SECRET)
            .config(f"spark.sql.catalog.{cat}.client.region", config.S3_REGION)
            .config("spark.sql.defaultCatalog", cat)
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
