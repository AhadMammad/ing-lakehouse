"""OOP PySpark medallion ETL for the `auth` identity-service source.

Reads the Postgres `auth` database and lands it into the Iceberg medallion
on Nessie + RustFS across dedicated, isolated namespaces:
  auth_bronze / auth_silver / auth_gold

Run as: python -m etl_app_spark --job {bronze,silver,gold} --table NAME --date YYYY-MM-DD
"""
