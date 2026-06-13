"""Entry point for the Auth identity-service historical data generator.

Populates the `auth` source database (same postgres-source container as
payments/rideon). Set PG_DB=auth when invoking.

Usage (docker, image ENTRYPOINT is `python -m`):
    docker run --rm -e PG_DB=auth ... data_generator \
        data_generator.auth \
        --start-date 2024-01-01 \
        --end-date   2024-01-31 \
        --users-per-day 50

Usage (make):
    make run-auth-generator START=2024-01-01 END=2024-01-31 USERS=50
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from data_generator.db import auth_events_exist_for_date, get_conn
from data_generator.generators import auth_gen, seed_auth


def parse_args():
    p = argparse.ArgumentParser(description="Generate fake auth/identity data by date range")
    p.add_argument("--start-date", required=True, help="First date to generate (YYYY-MM-DD)")
    p.add_argument("--end-date",   required=True, help="Last date to generate, inclusive (YYYY-MM-DD)")
    p.add_argument("--users-per-day", type=int, default=50, help="New user signups per day (default 50)")
    return p.parse_args()


def main():
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end   = date.fromisoformat(args.end_date)

    if end < start:
        raise SystemExit("--end-date must be >= --start-date")

    conn = get_conn()

    pools = seed_auth.seed_all(conn)

    total_days = (end - start).days + 1
    print(f"Generating auth data for {total_days} day(s): {start} → {end} ({args.users_per_day} signups/day)")

    day = start
    skipped = 0
    generated = 0
    while day <= end:
        existing = auth_events_exist_for_date(conn, day)
        if existing > 0:
            print(f"  {day}: SKIP — {existing} login attempts already exist")
            skipped += 1
        else:
            auth_gen.generate_for_date(conn, day, args.users_per_day, pools)
            generated += 1
        day += timedelta(days=1)

    conn.close()
    print(f"\nDone. Generated: {generated} day(s), Skipped: {skipped} day(s)")


if __name__ == "__main__":
    main()
