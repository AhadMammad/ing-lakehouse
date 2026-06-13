"""Silver job — auth_bronze.*_raw → auth_silver.* (cleaned, SCD1 upsert)."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from etl_app_spark import catalog, meta
from etl_app_spark.jobs.base import SparkJob
from etl_app_spark.schemas.silver_specs import SPECS


class AuthSilverJob(SparkJob):
    layer = "silver"

    def __init__(self, spark, table, logical_date, run_id):
        super().__init__(spark, table, logical_date, run_id)
        if table not in SPECS:
            raise SystemExit(f"unknown silver table '{table}' (have: {', '.join(SPECS)})")
        self.spec = SPECS[table]

    def extract(self) -> DataFrame:
        bronze = catalog.table_fqn("bronze", self.spec.bronze)
        df = self.spark.table(bronze).where(F.col("logical_date") == F.to_date(F.lit(self.logical_date)))
        # Dedupe to the latest ingest per natural key.
        w = Window.partitionBy(*[F.col(k) for k in self.spec.keys]).orderBy(F.col("ingest_ts").desc())
        return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")

    def transform(self, df: DataFrame) -> DataFrame:
        cleaned = _CLEANERS[self.table](df)
        return meta.add_silver_meta(cleaned, self.spec.business_cols, self.logical_date)

    def load(self, df: DataFrame) -> None:
        fqn = catalog.table_fqn("silver", self.spec.name)
        catalog.upsert_iceberg(df, fqn, self.spec.keys)


# ── Per-table cleaning (returns business columns only) ────────────

def _clean_users(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("user_id"),
        F.lower(F.trim("username")).alias("username"),
        F.lower(F.trim("email")).alias("email"),
        F.col("email_verified"),
        F.lower(F.trim("status")).alias("status"),
        F.col("display_name"),
        F.upper(F.col("country")).alias("country"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("last_login_at"),
        (F.lower(F.trim("status")) == F.lit("locked")).alias("is_locked"),
    )


def _clean_roles(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("role_id"),
        F.lower(F.trim("name")).alias("name"),
        F.col("description"),
    )


def _clean_permissions(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("permission_id"),
        F.lower(F.trim("code")).alias("code"),
        F.col("description"),
    )


def _clean_sessions(df: DataFrame) -> DataFrame:
    revoked_or_exp = F.coalesce(F.col("revoked_at"), F.col("expires_at"))
    return df.select(
        F.col("session_id"),
        F.col("user_id"),
        F.col("created_at"),
        F.col("expires_at"),
        F.col("revoked_at"),
        F.upper(F.trim("auth_method")).alias("auth_method"),
        F.col("ip_address"),
        F.col("user_agent"),
        F.col("revoked_at").isNull().alias("is_active"),
        F.col("revoked_at").isNotNull().alias("was_revoked"),
        F.greatest(
            F.unix_timestamp(revoked_or_exp) - F.unix_timestamp(F.col("created_at")),
            F.lit(0),
        ).alias("session_duration_seconds"),
    )


def _clean_login_attempts(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("attempt_id"),
        F.col("user_id"),
        F.col("username_tried"),
        F.col("attempted_at"),
        F.col("success"),
        F.lower(F.trim("failure_reason")).alias("failure_reason"),
        F.col("mfa_challenged"),
        F.upper(F.trim("auth_method")).alias("auth_method"),
        F.col("ip_address"),
        F.when(F.col("success"), F.lit("success"))
         .otherwise(F.coalesce(F.lower(F.trim("failure_reason")), F.lit("unknown")))
         .alias("attempt_outcome"),
    )


def _clean_oauth_accounts(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("oauth_account_id"),
        F.col("user_id"),
        F.lower(F.trim("provider")).alias("provider"),
        F.col("provider_user_id"),
        F.col("linked_at"),
    )


def _clean_mfa_devices(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("mfa_device_id"),
        F.col("user_id"),
        F.lower(F.trim("device_type")).alias("device_type"),
        F.col("label"),
        F.col("confirmed"),
        F.col("created_at"),
        F.col("last_used_at"),
    )


_CLEANERS = {
    "users": _clean_users,
    "roles": _clean_roles,
    "permissions": _clean_permissions,
    "sessions": _clean_sessions,
    "login_attempts": _clean_login_attempts,
    "oauth_accounts": _clean_oauth_accounts,
    "mfa_devices": _clean_mfa_devices,
}