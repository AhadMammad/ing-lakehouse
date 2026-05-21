"""bronze.weather_raw — fetch one day from Open-Meteo and append as-is.

Append-only and idempotent-by-replay: the raw JSON payload is kept in
`raw_payload`, so re-running the same date adds another snapshot but does
not corrupt history. Silver dedupes downstream.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.open_meteo import CITY_NAME, BAKU_LAT, BAKU_LON, fetch_baku_hourly
from etl_app.schemas import (
    BRONZE_NS,
    BRONZE_PARTITION,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
)


def _flatten(payload: dict, ingest_ts: dt.datetime) -> list[dict]:
    hourly = payload["hourly"]
    times = hourly["time"]
    raw = json.dumps(payload, separators=(",", ":"))
    rows = []
    for i, ts in enumerate(times):
        # Open-Meteo returns naive ISO strings in the requested timezone (UTC).
        observation_ts = dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc)
        rows.append(
            {
                "city": CITY_NAME,
                "lat": BAKU_LAT,
                "lon": BAKU_LON,
                "observation_ts": observation_ts,
                "temperature_2m": hourly["temperature_2m"][i],
                "relative_humidity_2m": float(hourly["relative_humidity_2m"][i])
                    if hourly["relative_humidity_2m"][i] is not None else None,
                "wind_speed_10m": hourly["wind_speed_10m"][i],
                "precipitation": hourly["precipitation"][i],
                "pressure_msl": hourly["pressure_msl"][i],
                "cloud_cover": float(hourly["cloud_cover"][i])
                    if hourly["cloud_cover"][i] is not None else None,
                "ingest_ts": ingest_ts,
                "source": "open-meteo",
                "raw_payload": raw,
            }
        )
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
    payload = fetch_baku_hourly(date)
    rows = _flatten(payload, ingest_ts)
    if not rows:
        print(f"[bronze] no rows returned for {date}")
        return

    df = pl.DataFrame(rows)
    catalog = get_catalog()
    table = _load_or_create(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(BRONZE_SCHEMA))
    table.append(arrow)
    table.refresh()
    print(f"[bronze] appended {len(rows)} rows for {date}; snapshots={len(table.history())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
