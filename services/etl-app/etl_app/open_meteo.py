"""Open-Meteo client — free, no API key. Single function, single city."""
from __future__ import annotations

import requests

BAKU_LAT = 40.4093
BAKU_LON = 49.8671
CITY_NAME = "Baku"

_HOURLY_VARS = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "precipitation,"
    "pressure_msl,"
    "cloud_cover"
)

_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={BAKU_LAT}&longitude={BAKU_LON}"
    f"&hourly={_HOURLY_VARS}"
    "&start_date={date}&end_date={date}&timezone=UTC"
)


def fetch_baku_hourly(date: str) -> dict:
    """Fetch one day of hourly observations for Baku. `date` is YYYY-MM-DD."""
    resp = requests.get(_URL.format(date=date), timeout=30)
    resp.raise_for_status()
    return resp.json()
