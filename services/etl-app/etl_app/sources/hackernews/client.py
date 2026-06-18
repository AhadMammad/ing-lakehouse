from __future__ import annotations

from datetime import datetime, timezone
import requests

_BASE_URL = "https://hn.algolia.com/api/v1/search_by_date"


def _day_bounds(date: str) -> tuple[int, int]:
    dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = int(dt.timestamp())
    end = start + 86400
    return start, end


def fetch_stories(date: str) -> list[dict]:
    start, end = _day_bounds(date)
    hits, page, nb_pages = [], 0, 1

    while page < nb_pages:
        resp = requests.get(
            _BASE_URL,
            params={
                "tags": "story",
                "numericFilters": f"created_at_i>{start},created_at_i<{end}",
                "hitsPerPage": 1000,
                "page": page,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        hits.extend(data["hits"])
        nb_pages = data["nbPages"]
        page += 1

    return hits
