"""CoinGecko client — free tier, no API key.

Single endpoint: /coins/markets returns the live market state for the
top-N coins by market cap. CoinGecko doesn't offer dated lookups on the
free tier, so each run snapshots "now" and the caller stamps the
logical-date.
"""
from __future__ import annotations

import requests

TOP_N = 20
VS_CURRENCY = "usd"
SOURCE_NAME = "coingecko"

_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    f"?vs_currency={VS_CURRENCY}&order=market_cap_desc&per_page={TOP_N}&page=1"
    "&sparkline=false&price_change_percentage=24h,7d"
)


def fetch_top_markets() -> list[dict]:
    """Return the top-N coins by market cap. Each element is one coin's market dict."""
    resp = requests.get(_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()
