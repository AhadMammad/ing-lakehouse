"""silver.<dim|fact>_* — Kimball star schema, SCD Type 1 dims.

`--table` dispatches to a per-table transform.  Dims read the
`logical_date == --date` slice from the snapshot bronze table, dedupe
on natural key (latest `ingest_ts` wins), and upsert.  Facts read the
incremental slice, derive the event date for partitioning, and upsert
on PK.
"""
from __future__ import annotations

import argparse
import datetime as dt

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import get_catalog
from etl_app.jobs._iceberg_utils import load_or_create
from etl_app.sources.postgres.schemas import (
    BRONZE_SPECS,
    SILVER_SPECS,
    SilverSpec,
)

UTC = dt.timezone.utc


def _read_bronze_slice(catalog, spec: SilverSpec, date: str) -> pl.DataFrame:
    bronze_id = BRONZE_SPECS[spec.bronze_key].identifier
    table = catalog.load_table(bronze_id)
    table.refresh()
    arrow = table.scan(row_filter=EqualTo("logical_date", date)).to_arrow()
    return pl.from_arrow(arrow)


def _dedup_latest(df: pl.DataFrame, key: list[str]) -> pl.DataFrame:
    return (
        df.sort("ingest_ts", descending=True)
        .unique(subset=key, keep="first")
    )


def _age_band(birthdate_col: pl.Expr, ref_date: dt.date) -> pl.Expr:
    age = (
        pl.lit(ref_date.year) - birthdate_col.dt.year()
        - ((pl.lit(ref_date.month) < birthdate_col.dt.month())
           | ((pl.lit(ref_date.month) == birthdate_col.dt.month())
              & (pl.lit(ref_date.day) < birthdate_col.dt.day()))).cast(pl.Int32)
    )
    return (
        pl.when(birthdate_col.is_null()).then(None)
        .when(age < 25).then(pl.lit("<25"))
        .when(age <= 34).then(pl.lit("25-34"))
        .when(age <= 44).then(pl.lit("35-44"))
        .when(age <= 54).then(pl.lit("45-54"))
        .otherwise(pl.lit("55+"))
        .alias("age_band")
    )


# ── dim transforms ────────────────────────────────────────────────


def _dim_customer(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["dim_customer"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    ref = dt.date.fromisoformat(date)
    return (
        _dedup_latest(src, ["customer_id"])
        .with_columns(
            _age_band(pl.col("birthdate"), ref),
            pl.lit(run_ts).alias("updated_at"),
        )
        .select([f.name for f in spec.schema.fields])
    )


def _dim_merchant(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["dim_merchant"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    return (
        _dedup_latest(src, ["merchant_id"])
        .with_columns(pl.lit(run_ts).alias("updated_at"))
        .select([f.name for f in spec.schema.fields])
    )


def _dim_payment_method(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["dim_payment_method"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    ref = dt.date.fromisoformat(date)
    return (
        _dedup_latest(src, ["method_id"])
        .with_columns(
            (
                (pl.col("expiry_year") < ref.year)
                | (
                    (pl.col("expiry_year") == ref.year)
                    & (pl.col("expiry_month") < ref.month)
                )
            ).alias("is_expired"),
            pl.lit(run_ts).alias("updated_at"),
        )
        .select([f.name for f in spec.schema.fields])
    )


def _dim_date(_catalog, date: str, _run_ts: dt.datetime) -> pl.DataFrame:
    d = dt.date.fromisoformat(date)
    return pl.DataFrame(
        [{
            "date": d,
            "day": d.day,
            "month": d.month,
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "day_of_week": d.isoweekday(),
            "day_name": d.strftime("%A"),
            "is_weekend": d.isoweekday() >= 6,
            "iso_week": d.isocalendar().week,
        }],
        schema_overrides={
            "day": pl.Int32, "month": pl.Int32, "quarter": pl.Int32,
            "year": pl.Int32, "day_of_week": pl.Int32, "iso_week": pl.Int32,
        },
    )


# ── fact transforms ───────────────────────────────────────────────


def _fact_payments(catalog, date: str, _run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["fact_payments"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    return (
        _dedup_latest(src, ["payment_id"])
        .with_columns(
            pl.col("created_at").dt.date().alias("payment_date"),
            (
                (pl.col("settled_at").cast(pl.Datetime("us")) - pl.col("created_at").cast(pl.Datetime("us")))
                .dt.total_seconds() / 3600.0
            ).alias("settled_lag_hours"),
        )
        .select([f.name for f in spec.schema.fields])
    )


def _fact_refunds(catalog, date: str, _run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["fact_refunds"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    return (
        _dedup_latest(src, ["refund_id"])
        .with_columns(pl.col("created_at").dt.date().alias("refund_date"))
        .select([f.name for f in spec.schema.fields])
    )


def _fact_fees(catalog, date: str, _run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["fact_fees"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    return (
        _dedup_latest(src, ["fee_id"])
        .with_columns(pl.col("created_at").dt.date().alias("fee_date"))
        .select([f.name for f in spec.schema.fields])
    )


def _fact_settlements(catalog, date: str, _run_ts: dt.datetime) -> pl.DataFrame:
    spec = SILVER_SPECS["fact_settlements"]
    src = _read_bronze_slice(catalog, spec, date)
    if src.is_empty():
        return src
    return (
        _dedup_latest(src, ["settlement_id"])
        .select([f.name for f in spec.schema.fields])
    )


TRANSFORMS = {
    "dim_customer": _dim_customer,
    "dim_merchant": _dim_merchant,
    "dim_payment_method": _dim_payment_method,
    "dim_date": _dim_date,
    "fact_payments": _fact_payments,
    "fact_refunds": _fact_refunds,
    "fact_fees": _fact_fees,
    "fact_settlements": _fact_settlements,
}


def main(table: str, date: str) -> None:
    if table not in TRANSFORMS:
        raise SystemExit(f"unknown silver table {table!r}; one of {sorted(TRANSFORMS)}")
    spec = SILVER_SPECS[table]
    run_ts = dt.datetime.now(tz=UTC)
    catalog = get_catalog()

    df = TRANSFORMS[table](catalog, date, run_ts)
    if df.is_empty():
        print(f"[payments_silver:{table}] no source rows for {date}")
        return

    table_ref = load_or_create(catalog, spec.identifier, spec.schema, spec.partition_spec)
    arrow = df.to_arrow().cast(schema_to_pyarrow(spec.schema))
    result = table_ref.upsert(arrow, join_cols=spec.join_cols)
    table_ref.refresh()
    print(
        f"[payments_silver:{table}] {date}: inserted={result.rows_inserted} "
        f"updated={result.rows_updated} snapshots={len(table_ref.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=sorted(TRANSFORMS))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    args = parser.parse_args()
    main(args.table, args.date)
