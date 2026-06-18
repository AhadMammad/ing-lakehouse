"""bronze.airquality_hourly_raw — fetch one day of hourly sensor readings from OpenAQ.

For each configured location, resolves its sensors, then fetches hourly
aggregates per sensor for --date. Appended rows are idempotent-by-replay via
raw_payload; silver deduplicates downstream on (sensor_id, hour_utc).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.config import OPENAQ_API_KEY, OPENAQ_LOCATION_IDS
from etl_app.sources.airquality.client import fetch_location_sensors, fetch_sensor_hours
from etl_app.sources.airquality.schemas import (
    BRONZE_NS,
    BRONZE_PARTITION,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
)


def _flatten(
    location: dict,
    sensor: dict,
    hours: list[dict],
    ingest_ts: dt.datetime,
    logical_date: dt.date,
) -> list[dict]:
    rows = []
    for h in hours:
        period = h.get("period") or {}
        datetime_from = (period.get("datetimeFrom") or {}).get("utc")
        summary = h.get("summary") or {}
        coverage = h.get("coverage") or {}

        if not datetime_from:
            continue
        hour_utc = dt.datetime.fromisoformat(datetime_from)
        observed_raw = coverage.get("observedCount")
        rows.append({
            "sensor_id": sensor["id"],
            "location_id": location["location_id"],
            "location_name": location["location_name"],
            "country_code": location["country_code"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "parameter_name": sensor["parameter_name"],
            "parameter_units": sensor["parameter_units"],
            "parameter_id": sensor["parameter_id"],
            "hour_utc": hour_utc,
            "value": h.get("value"),
            "summary_min": summary.get("min"),
            "summary_max": summary.get("max"),
            "summary_sd": summary.get("sd"),
            "percent_coverage": coverage.get("percentComplete"),
            "observed_count": int(observed_raw) if observed_raw is not None else None,
            "ingest_ts": ingest_ts,
            "logical_date": logical_date,
            "source": "openaq",
            "raw_payload": json.dumps(h, separators=(",", ":")),
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
    if not OPENAQ_LOCATION_IDS:
        print("[airquality_bronze] OPENAQ_LOCATION_IDS is empty — nothing to ingest")
        return
    if not OPENAQ_API_KEY:
        print("[airquality_bronze] OPENAQ_API_KEY is not set")
        return

    ingest_ts = dt.datetime.now(tz=dt.timezone.utc)
    logical_date = dt.date.fromisoformat(date)

    rows: list[dict] = []
    for location_id in OPENAQ_LOCATION_IDS:
        location = fetch_location_sensors(location_id, OPENAQ_API_KEY)
        for sensor in location["sensors"]:
            hours = fetch_sensor_hours(sensor["id"], date, OPENAQ_API_KEY)
            rows.extend(_flatten(location, sensor, hours, ingest_ts, logical_date))

    if not rows:
        print(f"[airquality_bronze] no measurements returned for {date}")
        return

    df = pl.DataFrame(rows)
    catalog = get_catalog()
    table = _load_or_create(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(BRONZE_SCHEMA))
    table.append(arrow)
    table.refresh()
    print(
        f"[airquality_bronze] appended {len(rows)} rows for {date}; "
        f"snapshots={len(table.history())}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
