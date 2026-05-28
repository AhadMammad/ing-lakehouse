"""Entry point for the data generator.

Usage (docker):
    docker run --rm ... data_generator \
        --start-date 2024-01-01 \
        --end-date   2024-01-31 \
        --rows-per-day 1000

Usage (make):
    make run-data-generator START=2024-01-01 END=2024-01-31 ROWS=1000
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from data_generator.db import get_conn, payments_exist_for_date
from data_generator.generators.seed import seed_all
from data_generator.generators.payments import generate_for_date


def parse_args():
    p = argparse.ArgumentParser(description="Generate fake payments platform data by date range")
    p.add_argument("--start-date", required=True, help="First date to generate (YYYY-MM-DD)")
    p.add_argument("--end-date",   required=True, help="Last date to generate, inclusive (YYYY-MM-DD)")
    p.add_argument("--rows-per-day", type=int, default=1000, help="Payments to generate per day (default 1000)")
    return p.parse_args()


def main():
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end   = date.fromisoformat(args.end_date)

    if end < start:
        raise SystemExit("--end-date must be >= --start-date")

    conn = get_conn()

    pools = seed_all(conn)
    merchant_data = pools["merchant_data"]
    customer_data = pools["customer_data"]
    method_rows   = pools["method_rows"]

    total_days = (end - start).days + 1
    print(f"Generating data for {total_days} day(s): {start} → {end} ({args.rows_per_day} rows/day)")

    day = start
    skipped = 0
    generated = 0
    while day <= end:
        existing = payments_exist_for_date(conn, day)
        if existing > 0:
            print(f"  {day}: SKIP — {existing} rows already exist")
            skipped += 1
        else:
            generate_for_date(conn, day, args.rows_per_day, merchant_data, customer_data, method_rows)
            generated += 1
        day += timedelta(days=1)

    conn.close()
    print(f"\nDone. Generated: {generated} day(s), Skipped: {skipped} day(s)")


if __name__ == "__main__":
    main()
