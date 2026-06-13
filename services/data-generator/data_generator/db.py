"""Database connection factory and shared helpers."""
from __future__ import annotations

import psycopg2
from psycopg2.extras import execute_values

from data_generator import config


def get_conn():
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DB,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
    )


def payments_exist_for_date(conn, date) -> int:
    """Return row count for `date` — used for idempotency check."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM payments WHERE created_at::date = %s",
            (date,),
        )
        return cur.fetchone()[0]


def rides_exist_for_date(conn, date) -> int:
    """Return ride count for `date` — used for the rideon idempotency check."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM rides WHERE requested_at::date = %s",
            (date,),
        )
        return cur.fetchone()[0]


def users_exist(conn) -> int:
    """Return the current user count — used to decide whether to seed the cohort."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]


def auth_events_exist_for_date(conn, date) -> int:
    """Return login_attempt count for `date` — used for the auth idempotency check."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM login_attempts WHERE attempted_at::date = %s",
            (date,),
        )
        return cur.fetchone()[0]


def bulk_insert(conn, table: str, rows: list[tuple], columns: list[str]) -> None:
    if not rows:
        return
    col_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
