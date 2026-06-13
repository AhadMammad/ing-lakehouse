"""Real-time auth activity simulator.

Continuously emits live auth events (logins, sessions, signups, status
changes, audit) into the `auth` source database at a configurable rate.
Runs as a long-lived docker-compose service (profile `realtime`) or via the
`make run-auth-realtime` target. Handles SIGTERM/SIGINT so `docker stop`
(and `make down-auth-realtime`) shut it down cleanly.

Usage (docker, image ENTRYPOINT is `python -m`):
    docker run --rm -e PG_DB=auth ... data_generator \
        data_generator.auth_realtime --interval-seconds 5 --rate 3

Usage (make):
    make run-auth-realtime
"""
from __future__ import annotations

import argparse
import signal
import time
from datetime import datetime, timezone

from data_generator.db import get_conn
from data_generator.generators import auth_gen, seed_auth

_running = True


def _handle_stop(signum, _frame):
    global _running
    print(f"\nReceived signal {signum} — shutting down after current tick...")
    _running = False


def parse_args():
    p = argparse.ArgumentParser(description="Continuously simulate real-time auth activity")
    p.add_argument("--interval-seconds", type=float, default=5.0, help="Seconds between ticks (default 5)")
    p.add_argument("--rate", type=int, default=3, help="Login attempts per tick (default 3)")
    p.add_argument("--max-ticks", type=int, default=0, help="Stop after N ticks (0 = run forever)")
    return p.parse_args()


def main():
    args = parse_args()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    conn = get_conn()
    pools = seed_auth.seed_all(conn)

    print(f"Real-time auth simulation started — rate={args.rate}/tick, interval={args.interval_seconds}s")
    tick = 0
    while _running:
        now = datetime.now(timezone.utc)
        counts = auth_gen.generate_tick(conn, pools, args.rate, now)
        tick += 1
        print(
            f"[tick {tick} @ {now:%H:%M:%S}] "
            + " ".join(f"{k}={v}" for k, v in counts.items() if v)
        )
        if args.max_ticks and tick >= args.max_ticks:
            break
        # Sleep in small slices so a stop signal is honoured promptly.
        slept = 0.0
        while _running and slept < args.interval_seconds:
            time.sleep(min(0.5, args.interval_seconds - slept))
            slept += 0.5

    conn.close()
    print(f"Real-time auth simulation stopped after {tick} tick(s).")


if __name__ == "__main__":
    main()
