"""bronze.rideon_*_raw — land rideon source tables into Iceberg.

`--table` selects one of the 10 BRONZE_SPECS entries. Reference tables
(cities/vehicle_categories/riders/drivers/vehicles) run as full snapshots
and overwrite the `logical_date` slice; transactional tables (rides/fares/
ride_payments/ratings/driver_payouts) run incrementally and append rows
whose `updated_at`/`created_at`/`payout_date` falls on `--date`.

Requires PG_DB=rideon in the environment (set by the DAG / Make target);
the connectorx reader resolves the database from that env var.

Idempotent: snapshot mode overwrites by `logical_date`; incremental mode
is dedupable downstream by PK in the dbt staging models.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import get_catalog
from etl_app.jobs._iceberg_utils import load_or_create, write_with_retry
from etl_app.sources.rideon.client import SOURCE_NAME, read_sql
from etl_app.sources.rideon.schemas import BRONZE_SPECS


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
    raw = df.select(source_cols).to_dicts()
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
        print(f"[rideon_bronze:{table}] no source rows for {date}")
        # Ensure the (empty) table still exists so downstream dbt sources
        # resolve even for a date/table with no rows. Snapshot mode also bails
        # here rather than overwriting an existing slice with nothing.
        load_or_create(get_catalog(), spec.identifier, spec.schema, spec.partition_spec)
        return

    target_int_cols = {
        f.name for f in spec.schema.fields
        if f.field_type.__class__.__name__ == "IntegerType"
    }
    df = _coerce_int_columns(df, target_int_cols)
    df = _enrich(df, spec.source_cols, ingest_ts, logical_date)
    df = df.select([f.name for f in spec.schema.fields])

    catalog = get_catalog()
    arrow = df.to_arrow().cast(schema_to_pyarrow(spec.schema))

    overwrite_filter = EqualTo("logical_date", date) if spec.mode == "snapshot" else None
    table_ref = write_with_retry(
        catalog, spec.identifier, spec.schema, spec.partition_spec,
        arrow, spec.mode, overwrite_filter=overwrite_filter,
    )
    verb = "overwrote" if spec.mode == "snapshot" else "appended"
    table_ref.refresh()
    print(
        f"[rideon_bronze:{table}] {verb} {df.height} rows for {date}; "
        f"snapshots={len(table_ref.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=sorted(BRONZE_SPECS))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (logical day)")
    args = parser.parse_args()
    main(args.table, args.date)
