"""Gold job — auth_silver.* → auth_gold star schema.

Dimensions: dim_date, dim_role, dim_auth_method (SCD1), dim_user (SCD2).
Facts: fact_login_attempts, fact_sessions (point-in-time FK to dim_user).

Overrides the base run() because gold builds one named dim/fact per call
rather than a single extract→transform→load chain.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_app_spark import catalog
from etl_app_spark.jobs.base import SparkJob
from etl_app_spark.meta import record_hash
from etl_app_spark.schemas.gold_specs import AUTH_METHOD_FAMILY, SCD2_TRACKED_ATTRS

_UNKNOWN_SK = "-1"
_SCD2_END = "9999-12-31 00:00:00"


class AuthGoldJob(SparkJob):
    layer = "gold"

    def run(self) -> None:
        catalog.ensure_namespace(self.spark, self.layer)
        handler = {
            "dim_date": self._dim_date,
            "dim_role": self._dim_role,
            "dim_auth_method": self._dim_auth_method,
            "dim_user": self._dim_user,
            "fact_login_attempts": self._fact_login_attempts,
            "fact_sessions": self._fact_sessions,
        }.get(self.table)
        if handler is None:
            raise SystemExit(f"unknown gold table '{self.table}'")
        n = handler()
        print(f"[gold:{self.table}] date={self.logical_date} run_id={self.run_id} rows={n}")

    # Abstract members are unused (run() is overridden) but must be defined.
    def extract(self):  # pragma: no cover
        raise NotImplementedError

    def transform(self, df):  # pragma: no cover
        raise NotImplementedError

    def load(self, df):  # pragma: no cover
        raise NotImplementedError

    # ── helpers ──
    def _silver(self, name: str) -> DataFrame:
        return self.spark.table(catalog.table_fqn("silver", name))

    def _bronze(self, name: str) -> DataFrame:
        return self.spark.table(catalog.table_fqn("bronze", name))

    def _upsert(self, df: DataFrame, name: str, keys: list[str], partition_col: str | None = None) -> int:
        fqn = catalog.table_fqn("gold", name)
        spark = self.spark
        if not catalog.table_exists(spark, fqn):
            writer = df.writeTo(fqn).using("iceberg")
            if partition_col:
                writer = writer.partitionedBy(F.col(partition_col))
            writer.create()
            return df.count()
        view = f"_gsrc_{name}"
        df.createOrReplaceTempView(view)
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        spark.sql(
            f"MERGE INTO {fqn} t USING {view} s ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
        spark.catalog.dropTempView(view)
        return df.count()

    # ── dimensions ──
    def _dim_date(self) -> int:
        df = self.spark.sql(
            f"SELECT explode(sequence(to_date('2020-01-01'), "
            f"to_date('{self.logical_date}'), interval 1 day)) AS full_date"
        )
        out = df.select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
            F.current_timestamp().alias("_loaded_at"),
        )
        return self._upsert(out, "dim_date", ["date_key"])

    def _dim_role(self) -> int:
        roles = self._silver("roles")
        rp = (
            self._bronze("role_permissions_raw")
            .where(F.col("logical_date") == F.to_date(F.lit(self.logical_date)))
            .groupBy("role_id")
            .agg(F.countDistinct("permission_id").alias("permission_count"))
        )
        out = (
            roles.join(rp, "role_id", "left")
            .select(
                F.col("role_id"),
                F.col("name").alias("role_name"),
                F.col("description"),
                F.coalesce(F.col("permission_count"), F.lit(0)).alias("permission_count"),
            )
            .withColumn("_record_hash", record_hash(["role_name", "description", "permission_count"]))
            .withColumn("_loaded_at", F.current_timestamp())
        )
        return self._upsert(out, "dim_role", ["role_id"])

    def _dim_auth_method(self) -> int:
        methods = (
            self._silver("sessions").select("auth_method")
            .union(self._silver("login_attempts").select("auth_method"))
            .where(F.col("auth_method").isNotNull())
            .distinct()
        )
        family = F.create_map(*[x for kv in AUTH_METHOD_FAMILY.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
        out = methods.select(
            F.md5(F.col("auth_method")).alias("auth_method_sk"),
            F.col("auth_method").alias("method_code"),
            F.coalesce(family[F.col("auth_method")], F.lit("other")).alias("method_family"),
            F.current_timestamp().alias("_loaded_at"),
        )
        return self._upsert(out, "dim_auth_method", ["auth_method_sk"])

    def _dim_user(self) -> int:
        """SCD Type 2 on dim_user (close-out current changed rows, then insert new versions)."""
        users = self._silver("users")
        # is_mfa_enabled — any confirmed device.
        mfa = (
            self._silver("mfa_devices").where(F.col("confirmed"))
            .groupBy("user_id").agg(F.lit(True).alias("is_mfa_enabled"))
        )
        # primary_role — earliest granted role per user.
        ur = (
            self._bronze("user_roles_raw")
            .where(F.col("logical_date") == F.to_date(F.lit(self.logical_date)))
        )
        roles = self._silver("roles").select(F.col("role_id"), F.col("name").alias("primary_role"))
        from pyspark.sql import Window
        w = Window.partitionBy("user_id").orderBy(F.col("granted_at").asc())
        primary = (
            ur.join(roles, "role_id", "left")
            .withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .select("user_id", "primary_role")
        )
        src = (
            users.join(mfa, "user_id", "left").join(primary, "user_id", "left")
            .select(
                F.col("user_id").alias("user_nk"),
                F.col("username"),
                F.col("email"),
                F.col("status"),
                F.col("country"),
                F.col("display_name"),
                F.coalesce(F.col("is_mfa_enabled"), F.lit(False)).alias("is_mfa_enabled"),
                F.coalesce(F.col("primary_role"), F.lit("user")).alias("primary_role"),
            )
            .withColumn("_record_hash", record_hash(SCD2_TRACKED_ATTRS))
            .withColumn("_effective_from", F.to_timestamp(F.lit(self.logical_date)))
        )

        fqn = catalog.table_fqn("gold", "dim_user")

        def _materialize(df: DataFrame) -> DataFrame:
            return df.select(
                F.md5(F.concat_ws("|", F.col("user_nk"), F.col("_effective_from").cast("string"))).alias("user_sk"),
                "user_nk", "username", "email", "status", "country", "display_name",
                "is_mfa_enabled", "primary_role", "_record_hash", "_effective_from",
                F.to_timestamp(F.lit(_SCD2_END)).alias("_effective_to"),
                F.lit(True).alias("_is_current"),
                F.current_timestamp().alias("_loaded_at"),
            )

        if not catalog.table_exists(self.spark, fqn):
            initial = _materialize(src)
            initial.writeTo(fqn).using("iceberg").create()
            return initial.count()

        # Step 1: close out current rows whose tracked attributes changed.
        src.createOrReplaceTempView("_dim_user_src")
        self.spark.sql(
            f"""
            MERGE INTO {fqn} t
            USING _dim_user_src s
              ON t.user_nk = s.user_nk AND t._is_current = true
            WHEN MATCHED AND t._record_hash <> s._record_hash THEN
              UPDATE SET t._is_current = false, t._effective_to = s._effective_from
            """
        )
        # Step 2: insert brand-new users + new versions of just-closed users.
        current = self.spark.table(fqn).where(F.col("_is_current")).select(
            F.col("user_nk").alias("c_nk"), F.col("_record_hash").alias("c_hash")
        )
        new_versions = (
            src.join(current, src.user_nk == current.c_nk, "left")
            .where(F.col("c_nk").isNull() | (F.col("c_hash") != F.col("_record_hash")))
            .drop("c_nk", "c_hash")
        )
        to_insert = _materialize(new_versions)
        n = to_insert.count()
        if n:
            to_insert.writeTo(fqn).append()
        self.spark.catalog.dropTempView("_dim_user_src")
        return n

    # ── facts ──
    def _dim_user_pit(self) -> DataFrame | None:
        """Point-in-time slice of dim_user for FK resolution, or None if not built yet."""
        fqn = catalog.table_fqn("gold", "dim_user")
        if not catalog.table_exists(self.spark, fqn):
            return None
        return self.spark.table(fqn).select(
            "user_sk", "user_nk", "_effective_from", "_effective_to"
        )

    def _fact_login_attempts(self) -> int:
        src = self._silver("login_attempts").where(
            F.col("_source_logical_date") == F.to_date(F.lit(self.logical_date))
        )
        dim = self._dim_user_pit()
        if dim is not None:
            joined = src.join(
                dim,
                (src.user_id == dim.user_nk)
                & (dim._effective_from <= src.attempted_at)
                & (src.attempted_at < dim._effective_to),
                "left",
            )
            user_sk = F.coalesce(F.col("user_sk"), F.lit(_UNKNOWN_SK))
        else:
            joined = src
            user_sk = F.lit(_UNKNOWN_SK)
        out = joined.select(
            F.col("attempt_id"),
            user_sk.alias("user_sk"),
            F.date_format("attempted_at", "yyyyMMdd").cast("int").alias("date_key"),
            F.md5(F.col("auth_method")).alias("auth_method_sk"),
            F.col("attempted_at"),
            F.col("success").cast("int").alias("is_success"),
            F.col("mfa_challenged").cast("int").alias("is_mfa_challenged"),
            F.col("failure_reason"),
            F.to_date("attempted_at").alias("attempt_date"),
            F.current_timestamp().alias("_loaded_at"),
        )
        return self._upsert(out, "fact_login_attempts", ["attempt_id"], partition_col="attempt_date")

    def _fact_sessions(self) -> int:
        src = self._silver("sessions").where(
            F.col("_source_logical_date") == F.to_date(F.lit(self.logical_date))
        )
        dim = self._dim_user_pit()
        if dim is not None:
            joined = src.join(
                dim,
                (src.user_id == dim.user_nk)
                & (dim._effective_from <= src.created_at)
                & (src.created_at < dim._effective_to),
                "left",
            )
            user_sk = F.coalesce(F.col("user_sk"), F.lit(_UNKNOWN_SK))
        else:
            joined = src
            user_sk = F.lit(_UNKNOWN_SK)
        out = joined.select(
            F.col("session_id"),
            user_sk.alias("user_sk"),
            F.date_format("created_at", "yyyyMMdd").cast("int").alias("date_key"),
            F.md5(F.col("auth_method")).alias("auth_method_sk"),
            F.col("created_at"),
            F.col("session_duration_seconds"),
            F.col("is_active").cast("int").alias("is_active"),
            F.col("was_revoked").cast("int").alias("was_revoked"),
            F.to_date("created_at").alias("session_date"),
            F.current_timestamp().alias("_loaded_at"),
        )
        return self._upsert(out, "fact_sessions", ["session_id"], partition_col="session_date")
