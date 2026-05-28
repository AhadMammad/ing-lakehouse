"""gold.* — aggregate marts over the silver star schema.

`--mart` dispatches to one of 4 compute functions.  Three marts
overwrite a single `day`/`settlement_date` slice; `customer_lifetime_value`
is a whole-table recompute.
"""
from __future__ import annotations

import argparse
import datetime as dt
from decimal import Decimal

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import get_catalog
from etl_app.jobs._iceberg_utils import load_or_create
from etl_app.sources.postgres.schemas import GOLD_SPECS, SILVER_SPECS

UTC = dt.timezone.utc
MONEY = pl.Decimal(precision=14, scale=2)
ZERO = Decimal("0.00")


def _read_silver(catalog, key: str, row_filter=None) -> pl.DataFrame:
    spec = SILVER_SPECS[key]
    table = catalog.load_table(spec.identifier)
    table.refresh()
    scan = table.scan(row_filter=row_filter) if row_filter is not None else table.scan()
    return pl.from_arrow(scan.to_arrow())


def _money(value) -> Decimal:
    return Decimal(str(value)) if value is not None else ZERO


# ── marts ─────────────────────────────────────────────────────────


def _daily_payment_summary(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    day = dt.date.fromisoformat(date)
    payments = _read_silver(catalog, "fact_payments", EqualTo("payment_date", date))
    if payments.is_empty():
        return payments
    refunds = _read_silver(catalog, "fact_refunds", EqualTo("refund_date", date))
    fees = _read_silver(catalog, "fact_fees", EqualTo("fee_date", date))

    total_volume = _money(payments["amount"].sum())
    refund_volume = _money(refunds["amount"].sum()) if not refunds.is_empty() else ZERO
    fee_volume = _money(fees["amount"].sum()) if not fees.is_empty() else ZERO
    successful = payments.filter(pl.col("status") == "completed").height
    failed = payments.filter(pl.col("status") == "failed").height
    total = payments.height
    avg_amount = (total_volume / total) if total else ZERO

    return pl.DataFrame(
        [{
            "day": day,
            "total_payments": total,
            "total_volume": total_volume,
            "successful_count": successful,
            "failed_count": failed,
            "success_rate_pct": (100.0 * successful / total) if total else 0.0,
            "avg_amount": avg_amount.quantize(Decimal("0.01")),
            "unique_customers": payments["customer_id"].n_unique(),
            "unique_merchants": payments["merchant_id"].n_unique(),
            "refund_volume": refund_volume,
            "fee_volume": fee_volume,
            "net_revenue": total_volume - refund_volume - fee_volume,
            "computed_at": run_ts,
        }],
        schema_overrides={
            "total_payments": pl.Int32, "successful_count": pl.Int32,
            "failed_count": pl.Int32, "unique_customers": pl.Int32,
            "unique_merchants": pl.Int32,
            "total_volume": MONEY, "avg_amount": MONEY,
            "refund_volume": MONEY, "fee_volume": MONEY, "net_revenue": MONEY,
        },
    )


def _daily_revenue_by_merchant(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    payments = _read_silver(catalog, "fact_payments", EqualTo("payment_date", date))
    if payments.is_empty():
        return payments
    refunds = _read_silver(catalog, "fact_refunds", EqualTo("refund_date", date))
    fees = _read_silver(catalog, "fact_fees", EqualTo("fee_date", date))
    merchants = _read_silver(catalog, "dim_merchant")

    refund_per_payment = (
        refunds.group_by("payment_id").agg(pl.col("amount").sum().alias("refund_amount"))
        if not refunds.is_empty()
        else pl.DataFrame(schema={"payment_id": pl.Utf8, "refund_amount": MONEY})
    )
    fee_per_payment = (
        fees.group_by("payment_id").agg(pl.col("amount").sum().alias("fee_amount"))
        if not fees.is_empty()
        else pl.DataFrame(schema={"payment_id": pl.Utf8, "fee_amount": MONEY})
    )

    enriched = (
        payments.join(refund_per_payment, on="payment_id", how="left")
        .join(fee_per_payment, on="payment_id", how="left")
        .with_columns(
            pl.col("refund_amount").fill_null(ZERO).cast(MONEY),
            pl.col("fee_amount").fill_null(ZERO).cast(MONEY),
        )
    )

    by_merchant = (
        enriched.group_by("merchant_id")
        .agg(
            pl.col("amount").sum().alias("gross_volume"),
            pl.len().alias("txn_count"),
            (pl.col("status") == "completed").sum().alias("success_count"),
            pl.col("refund_amount").sum().alias("refund_amount"),
            pl.col("fee_amount").sum().alias("fee_amount"),
            pl.col("amount").mean().alias("avg_ticket"),
        )
        .with_columns(
            (pl.col("gross_volume") - pl.col("refund_amount") - pl.col("fee_amount"))
            .alias("net_revenue"),
        )
        .join(
            merchants.select(["merchant_id", "name", "category_code"]),
            on="merchant_id", how="left",
        )
        .rename({"name": "merchant_name"})
        .with_columns(
            pl.lit(dt.date.fromisoformat(date)).alias("day"),
            pl.lit(run_ts).alias("computed_at"),
            pl.col("txn_count").cast(pl.Int32),
            pl.col("success_count").cast(pl.Int32),
            pl.col("gross_volume").cast(MONEY),
            pl.col("refund_amount").cast(MONEY),
            pl.col("fee_amount").cast(MONEY),
            pl.col("net_revenue").cast(MONEY),
            pl.col("avg_ticket").cast(MONEY),
        )
        .select([f.name for f in GOLD_SPECS["daily_revenue_by_merchant"].schema.fields])
    )
    return by_merchant


def _customer_lifetime_value(catalog, _date: str, run_ts: dt.datetime) -> pl.DataFrame:
    payments = _read_silver(catalog, "fact_payments")
    if payments.is_empty():
        return payments
    refunds = _read_silver(catalog, "fact_refunds")
    customers = _read_silver(catalog, "dim_customer")

    refund_per_payment = (
        refunds.group_by("payment_id").agg(pl.col("amount").sum().alias("refund_amount"))
        if not refunds.is_empty()
        else pl.DataFrame(schema={"payment_id": pl.Utf8, "refund_amount": MONEY})
    )

    enriched = payments.join(refund_per_payment, on="payment_id", how="left").with_columns(
        pl.col("refund_amount").fill_null(ZERO).cast(MONEY),
    )

    by_customer = (
        enriched.group_by("customer_id")
        .agg(
            pl.col("amount").sum().alias("total_spend"),
            pl.len().alias("total_payments"),
            (pl.col("status") == "completed").sum().alias("successful_payments"),
            (pl.col("status") == "failed").sum().alias("failed_payments"),
            pl.col("refund_amount").sum().alias("total_refunds"),
            pl.col("payment_date").min().alias("first_payment_date"),
            pl.col("payment_date").max().alias("last_payment_date"),
            pl.col("payment_date").n_unique().alias("active_days"),
        )
        .with_columns(
            pl.when(pl.col("total_spend") > 0)
            .then(100.0 * pl.col("total_refunds").cast(pl.Float64) / pl.col("total_spend").cast(pl.Float64))
            .otherwise(0.0)
            .alias("refund_rate_pct"),
        )
        .join(
            customers.select([
                "customer_id", "first_name", "last_name", "country", "age_band",
            ]),
            on="customer_id", how="left",
        )
        .with_columns(
            (pl.col("first_name") + pl.lit(" ") + pl.col("last_name")).alias("full_name"),
            pl.lit(run_ts).alias("computed_at"),
            pl.col("total_payments").cast(pl.Int32),
            pl.col("successful_payments").cast(pl.Int32),
            pl.col("failed_payments").cast(pl.Int32),
            pl.col("active_days").cast(pl.Int32),
            pl.col("total_spend").cast(MONEY),
            pl.col("total_refunds").cast(MONEY),
        )
        .select([f.name for f in GOLD_SPECS["customer_lifetime_value"].schema.fields])
    )
    return by_customer


def _merchant_settlement_daily(catalog, date: str, run_ts: dt.datetime) -> pl.DataFrame:
    settlements = _read_silver(catalog, "fact_settlements", EqualTo("settlement_date", date))
    if settlements.is_empty():
        return settlements
    payments = _read_silver(catalog, "fact_payments", EqualTo("payment_date", date))
    merchants = _read_silver(catalog, "dim_merchant")

    gross_per_merchant = (
        payments.group_by("merchant_id").agg(pl.col("amount").sum().alias("gross_volume_for_date"))
        if not payments.is_empty()
        else pl.DataFrame(schema={"merchant_id": pl.Utf8, "gross_volume_for_date": MONEY})
    )

    enriched = (
        settlements.join(gross_per_merchant, on="merchant_id", how="left")
        .join(merchants.select(["merchant_id", "name", "created_at"]).rename(
            {"name": "merchant_name", "created_at": "_merch_created"}
        ), on="merchant_id", how="left")
        .with_columns(
            pl.col("amount").alias("settled_amount"),
            pl.col("payments_count").alias("settled_payments_count"),
            (pl.col("settlement_date") - pl.col("created_at").dt.date()).dt.total_days()
            .cast(pl.Int32).alias("settlement_lag_days"),
            pl.col("gross_volume_for_date").fill_null(ZERO).cast(MONEY),
            pl.lit(run_ts).alias("computed_at"),
        )
        .with_columns(
            pl.col("settled_amount").cast(MONEY),
            pl.col("settled_payments_count").cast(pl.Int32),
        )
        .select([f.name for f in GOLD_SPECS["merchant_settlement_daily"].schema.fields])
    )
    return enriched


MARTS = {
    "daily_payment_summary": _daily_payment_summary,
    "daily_revenue_by_merchant": _daily_revenue_by_merchant,
    "customer_lifetime_value": _customer_lifetime_value,
    "merchant_settlement_daily": _merchant_settlement_daily,
}


def main(mart: str, date: str) -> None:
    if mart not in MARTS:
        raise SystemExit(f"unknown gold mart {mart!r}; one of {sorted(MARTS)}")
    spec = GOLD_SPECS[mart]
    run_ts = dt.datetime.now(tz=UTC)
    catalog = get_catalog()

    df = MARTS[mart](catalog, date, run_ts)
    if df.is_empty():
        print(f"[payments_gold:{mart}] no silver rows for {date}")
        return

    table_ref = load_or_create(catalog, spec.identifier, spec.schema, spec.partition_spec)
    arrow = df.to_arrow().cast(schema_to_pyarrow(spec.schema))
    if spec.overwrite_column is None:
        table_ref.overwrite(arrow)
    else:
        table_ref.overwrite(arrow, overwrite_filter=EqualTo(spec.overwrite_column, date))
    table_ref.refresh()
    print(
        f"[payments_gold:{mart}] {date}: wrote {df.height} rows; "
        f"snapshots={len(table_ref.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True, choices=sorted(MARTS))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    args = parser.parse_args()
    main(args.mart, args.date)
