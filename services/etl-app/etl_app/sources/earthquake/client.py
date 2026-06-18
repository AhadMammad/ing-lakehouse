"""USGS Earthquake Hazards API client — free, no API key required."""
from __future__ import annotations

import requests

_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

_PARAMS = {
    "format": "geojson",
    "minmagnitude": "1.0",
    "orderby": "time",
    "limit": 20000,
}


def fetch_earthquakes(date: str) -> list[dict]:
    """Fetch all M≥1.0 global earthquakes for a UTC calendar day.

    `date` is YYYY-MM-DD; endtime is exclusive next-day midnight so the full
    day is captured without double-counting boundary events.
    """
    from datetime import date as _date, timedelta

    next_day = (_date.fromisoformat(date) + timedelta(days=1)).isoformat()
    resp = requests.get(
        _URL,
        params={**_PARAMS, "starttime": date, "endtime": next_day},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["features"]
