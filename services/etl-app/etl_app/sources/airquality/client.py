"""OpenAQ v3 API client — free API key required (openaq.org/register).

Authentication is via X-API-Key header. The API is sensor-centric:
  1. GET /locations/{location_id} → station metadata + sensor list
  2. GET /sensors/{sensor_id}/hours → hourly aggregates for a date range
"""
from __future__ import annotations

from datetime import date as _date, timedelta

import requests

_BASE = "https://api.openaq.org/v3"


def fetch_location_sensors(location_id: int, api_key: str) -> dict:
    """Return location metadata and its sensor list from OpenAQ.

    Returns a dict with keys: location_id, location_name, country_code,
    latitude, longitude, sensors (list of {id, parameter_name, parameter_units,
    parameter_id}).
    """
    resp = requests.get(
        f"{_BASE}/locations/{location_id}",
        headers={"X-API-Key": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()["results"][0]
    coords = data.get("coordinates") or {}
    return {
        "location_id": data["id"],
        "location_name": data.get("name"),
        "country_code": (data.get("country") or {}).get("code"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "sensors": [
            {
                "id": s["id"],
                "parameter_name": (s.get("parameter") or {}).get("name"),
                "parameter_units": (s.get("parameter") or {}).get("units"),
                "parameter_id": (s.get("parameter") or {}).get("id"),
            }
            for s in (data.get("sensors") or [])
        ],
    }


def fetch_sensor_hours(sensor_id: int, date: str, api_key: str) -> list[dict]:
    """Return hourly measurement aggregates for a sensor on a UTC calendar day.

    `date` is YYYY-MM-DD; the range [date, date+1day) captures the full day
    without boundary overlap. Returns raw result dicts from the API (period,
    value, summary, coverage).
    """
    next_day = (_date.fromisoformat(date) + timedelta(days=1)).isoformat()
    resp = requests.get(
        f"{_BASE}/sensors/{sensor_id}/hours",
        headers={"X-API-Key": api_key},
        params={
            "datetime_from": date,
            "datetime_to": next_day,
            "limit": 100,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])
