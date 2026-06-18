"""
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import polars as pl
from pyiceberg.io.pyarrow import schema_to_pyarrow

from etl_app.catalog import ensure_namespace, get_catalog
from etl_app.sources.hackernews.client import (fetch_stories,
)
from etl_app.sources.hackernews.schemas import (
    BRONZE_NS,
    BRONZE_PARTITION,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
)


def _flatten(hits: list[dict], ingest_ts: dt.datetime) -> list[dict]:
    rows= []   
    for hit in hits:
        rows.append(
            {
                "story_id": int(hit["objectID"]),
                "title": hit.get("title") or "",
                "author": hit.get("author"),
                "url": hit.get("url"),
                "points": hit.get("points"),
                "num_comments": hit.get("num_comments"),
                "created_at": hit.get("created_at"),
                "story_type": hit.get("story_type"),
                "ingest_ts": ingest_ts,
                "raw_payload": json.dumps(hit, separators=(",", ":")),
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
    payload = fetch_stories(date)
    rows = _flatten(payload, ingest_ts)
    if not rows:
        print(f"[hackernews_bronze] no rows returned for {date}")
        return

    df = pl.DataFrame(rows)
    catalog = get_catalog()
    table = _load_or_create(catalog)
    arrow = df.to_arrow().cast(schema_to_pyarrow(BRONZE_SCHEMA))
    table.append(arrow)
    table.refresh()
    print(f"[hackernews_bronze] appended {len(rows)} rows for {date}; snapshots={len(table.history())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    main(parser.parse_args().date)
