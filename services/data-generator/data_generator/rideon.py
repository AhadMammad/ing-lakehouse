"""Entry point for the Rideon ride-hailing data generator.

Populates the `rideon` source database (same postgres-source container as
payments). Set PG_DB=rideon when invoking.

Usage (docker, image ENTRYPOINT is `python -m`):
    docker run --rm -e PG_DB=rideon ... data_generator \
        data_generator.rideon \
        --start-date 2024-01-01 \
        --end-date   2024-01-31 \
        --rides-per-day 500

Usage (make):
    make run-rideon-generator START=2024-01-01 END=2024-01-31 RIDES=500
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from data_generator.db import get_conn, rides_exist_for_date
from data_generator.generators import rides_gen, seed_rideon


def parse_args():
    p = argparse.ArgumentParser(description="Generate fake ride-hailing data by date range")
    p.add_argument("--start-date", required=True, help="First date to generate (YYYY-MM-DD)")
    p.add_argument("--end-date",   required=True, help="Last date to generate, inclusive (YYYY-MM-DD)")
    p.add_argument("--rides-per-day", type=int, default=500, help="Rides to generate per day (default 500)")
    return p.parse_args()


def main():
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end   = date.fromisoformat(args.end_date)

    if end < start:
        raise SystemExit("--end-date must be >= --start-date")

    conn = get_conn()

    pools = seed_rideon.seed_all(conn)

    total_days = (end - start).days + 1
    print(f"Generating rideon data for {total_days} day(s): {start} → {end} ({args.rides_per_day} rides/day)")

    day = start
    skipped = 0
    generated = 0
    while day <= end:
        existing = rides_exist_for_date(conn, day)
        if existing > 0:
            print(f"  {day}: SKIP — {existing} rides already exist")
            skipped += 1
        else:
            rides_gen.generate_for_date(
                conn, day, args.rides_per_day,
                pools["city_data"],
                pools["category_rows"],
                pools["rider_data"],
                pools["driver_data"],
                pools["vehicle_data"],
            )
            generated += 1
        day += timedelta(days=1)

    conn.close()
    print(f"\nDone. Generated: {generated} day(s), Skipped: {skipped} day(s)")


if __name__ == "__main__":
    main()
