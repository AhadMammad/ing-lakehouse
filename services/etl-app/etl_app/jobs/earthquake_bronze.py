"""bronze.earthquake_raw — fetch one day from USGS and append as-is.

Append-only and idempotent-by-replay: the raw GeoJSON payload is kept in
`raw_payload`, so re-running the same date adds another snapshot but does
not corrupt history. Silver dedupes downstream on event_id.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.earthquake.client import fetch_earthquakes
from etl_app.sources.earthquake.schemas import (
    BRONZE_NS,
    BRONZE_PARTITION,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
)


def _flatten(features: list[dict], ingest_ts: dt.datetime, logical_date: dt.date) -> list[dict]:
    rows = []
    for feat in features:
        props = feat.get("properties") or {}
        coords = (feat.get("geometry") or {}).get("coordinates") or [None, None, None]

        # USGS timestamps are epoch milliseconds; convert to UTC datetime.
        event_ms = props.get("time")
        updated_ms = props.get("updated")
        event_time = (
            dt.datetime.fromtimestamp(event_ms / 1000, tz=dt.timezone.utc)
            if event_ms is not None else None
        )
        updated_time = (
            dt.datetime.fromtimestamp(updated_ms / 1000, tz=dt.timezone.utc)
            if updated_ms is not None else None
        )

        rows.append({
            "event_id": feat.get("id"),
            "mag": props.get("mag"),
            "mag_type": props.get("magType"),
            "place": props.get("place"),
            "event_time": event_time,
            "updated_time": updated_time,
            "latitude": coords[1],
            "longitude": coords[0],
            "depth_km": coords[2],
            "alert": props.get("alert"),
            "tsunami": int(props["tsunami"]) if props.get("tsunami") is not None else None,
            "sig": int(props["sig"]) if props.get("sig") is not None else None,
            "net": props.get("net"),
            "status": props.get("status"),
            "ingest_ts": ingest_ts,
            "logical_date": logical_date,
            "source": "usgs",
            "raw_payload": json.dumps(feat, separators=(",", ":")),
        })
    return rows


def _load_or_create(catalog):
    if catalog.table_exists(BRONZE_TABLE):
        return catalog.load_table(BRONZE_TABLE)
    ensure_namespace(catalog, BRONZE_NS)
    return catalog.create_table(
        identifier=BRONZE_TABLE,
        schema=BRONZE_SCHEMA,
        partition_spec=BRONZE_PARTITION,
    )


def main(date: str) -> None:
    ingest_ts = dt.datetime.now(tz=dt.timezone.utc)
    logical_date = dt.date.fromisoformat(date)

    features = fetch_earthquakes(date)
    if not features:
        print(f"[earthquake_bronze] no events returned for {date}")
        return

    rows = _flatten(features, ingest_ts, logical_date)
    df = pl.DataFrame(rows)
    catalog = get_catalog()
    table = _load_or_create(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(BRONZE_SCHEMA))
    table.append(arrow)
    table.refresh()
    print(
        f"[earthquake_bronze] appended {len(rows)} rows for {date}; "
        f"snapshots={len(table.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
