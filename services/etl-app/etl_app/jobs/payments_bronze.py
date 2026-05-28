"""bronze.payments_*_raw — land postgres-source tables into Iceberg.

`--table` selects one of the 7 BRONZE_SPECS entries. Dimensions
(merchants/customers/methods) run as full snapshots and overwrite the
`logical_date` slice; transactional tables (payments/refunds/fees/
settlements) run incrementally and append rows whose `updated_at`/
`created_at`/`settlement_date` falls on `--date`.

Idempotent: snapshot mode overwrites by `logical_date`; incremental
mode is dedupable downstream by PK in silver.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import get_catalog
from etl_app.jobs._iceberg_utils import load_or_create
from etl_app.sources.postgres.client import SOURCE_NAME, read_sql
from etl_app.sources.postgres.schemas import BRONZE_SPECS


def _json_default(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return str(value)


def _enrich(
    df: pl.DataFrame,
    source_cols: list[str],
    ingest_ts: dt.datetime,
    logical_date: dt.date,
) -> pl.DataFrame:
    raw = (
        df.select(source_cols)
        .to_dicts()
    )
    payloads = [json.dumps(row, default=_json_default, separators=(",", ":")) for row in raw]
    return df.with_columns(
        pl.lit(ingest_ts).alias("ingest_ts"),
        pl.lit(logical_date).alias("logical_date"),
        pl.lit(SOURCE_NAME).alias("source"),
        pl.Series("raw_payload", payloads, dtype=pl.Utf8),
    )


def _coerce_int_columns(df: pl.DataFrame, target_cols: set[str]) -> pl.DataFrame:
    """Iceberg IntegerType is 32-bit; PG SMALLINT comes back as Int16."""
    int_cols = [
        c for c in df.columns
        if c in target_cols and df.schema[c] in (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16)
    ]
    if not int_cols:
        return df
    return df.with_columns([pl.col(c).cast(pl.Int32) for c in int_cols])


def main(table: str, date: str) -> None:
    if table not in BRONZE_SPECS:
        raise SystemExit(f"unknown bronze table {table!r}; one of {sorted(BRONZE_SPECS)}")
    spec = BRONZE_SPECS[table]
    logical_date = dt.date.fromisoformat(date)
    ingest_ts = dt.datetime.now(tz=dt.timezone.utc)

    sql = spec.sql(date)
    df = read_sql(sql)

    if df.is_empty():
        print(f"[payments_bronze:{table}] no source rows for {date}")
        if spec.mode == "incremental":
            return
        # snapshot with empty source is suspicious — bail rather than wipe.
        return

    target_int_cols = {
        f.name for f in spec.schema.fields
        if f.field_type.__class__.__name__ == "IntegerType"
    }
    df = _coerce_int_columns(df, target_int_cols)
    df = _enrich(df, spec.source_cols, ingest_ts, logical_date)
    df = df.select([f.name for f in spec.schema.fields])

    catalog = get_catalog()
    table_ref = load_or_create(catalog, spec.identifier, spec.schema, spec.partition_spec)
    arrow = df.to_arrow().cast(schema_to_pyarrow(spec.schema))

    if spec.mode == "snapshot":
        table_ref.overwrite(arrow, overwrite_filter=EqualTo("logical_date", date))
        verb = "overwrote"
    else:
        table_ref.append(arrow)
        verb = "appended"
    table_ref.refresh()
    print(
        f"[payments_bronze:{table}] {verb} {df.height} rows for {date}; "
        f"snapshots={len(table_ref.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=sorted(BRONZE_SPECS))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    args = parser.parse_args()
    main(args.table, args.date)
