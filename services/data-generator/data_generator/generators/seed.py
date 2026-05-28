"""Idempotent seeding of the static reference pool.

Creates 50 merchants, 500 customers, and ~750 payment methods once.
All inserts use ON CONFLICT DO NOTHING so re-runs are safe. IDs and
created_at timestamps are returned from the DB so the payment generator
can filter by entity age and ensure payments only occur after both the
merchant and the customer existed.
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

_NOW = datetime.now(timezone.utc)

_CATEGORIES = [
    ("RETAIL",        "5411"),
    ("FOOD",          "5812"),
    ("TRAVEL",        "4722"),
    ("ENTERTAINMENT", "7929"),
    ("UTILITIES",     "4900"),
]

_COUNTRIES = ["US", "GB", "DE", "FR", "AZ", "NL", "CA", "AU", "SG", "AE"]

_METHOD_TYPES = [
    ("card",         "Visa",       "visa"),
    ("card",         "Mastercard", "mastercard"),
    ("card",         "Amex",       "amex"),
    ("bank_account", "IBAN",       None),
    ("wallet",       "PayPal",     None),
]


# ── Timestamp helpers ─────────────────────────────────────────────

def _random_birthdate() -> date:
    """Random birthdate for a customer aged 18–100 at seeding time."""
    max_bday = _NOW.date() - timedelta(days=18 * 365)
    min_bday = _NOW.date() - timedelta(days=100 * 365)
    span = (max_bday - min_bday).days
    return min_bday + timedelta(days=random.randint(0, span))


def _random_merchant_ts() -> datetime:
    """Merchants are established businesses: created 1–5 years ago."""
    days_ago = random.randint(365, 5 * 365)
    second_offset = random.randint(0, 86399)
    return _NOW - timedelta(days=days_ago, seconds=second_offset)


def _random_customer_ts() -> datetime:
    """Customers signed up 3 months–3 years ago."""
    days_ago = random.randint(90, 3 * 365)
    second_offset = random.randint(0, 86399)
    return _NOW - timedelta(days=days_ago, seconds=second_offset)


def _random_method_ts(customer_ts: datetime) -> datetime:
    """Payment method added after signup, within 1 year of signup or now."""
    latest = min(_NOW, customer_ts + timedelta(days=365))
    if latest <= customer_ts:
        return customer_ts
    span = int((latest - customer_ts).total_seconds())
    return customer_ts + timedelta(seconds=random.randint(0, span))


# ── Seed functions ────────────────────────────────────────────────

def seed_merchants(conn) -> list[tuple[str, datetime]]:
    """Insert 50 merchants and return (merchant_id, created_at) pairs."""
    rows = []
    for _ in range(50):
        cat, mcc = random.choice(_CATEGORIES)
        rows.append((
            fake.company()[:120],
            cat,
            mcc,
            random.choice(_COUNTRIES),
            fake.city()[:80],
            _random_merchant_ts(),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO merchants (name, category_code, mcc_code, country, city, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT merchant_id, created_at FROM merchants")
        return [(str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()]


def seed_customers(conn) -> list[tuple[str, datetime]]:
    """Insert 500 customers and return (customer_id, created_at) pairs."""
    rows = []
    emails_seen: set[str] = set()
    while len(rows) < 500:
        email = fake.unique.email()
        if email in emails_seen:
            continue
        emails_seen.add(email)
        rows.append((
            fake.first_name()[:80],
            fake.last_name()[:80],
            email,
            _random_birthdate(),
            fake.phone_number()[:30],
            random.choice(_COUNTRIES),
            fake.city()[:80],
            _random_customer_ts(),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO customers
              (first_name, last_name, email, birthdate, phone, country, city, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT customer_id, created_at FROM customers")
        return [(str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()]


def seed_payment_methods(
    conn,
    customer_data: list[tuple[str, datetime]],
) -> list[tuple[str, str]]:
    """Insert ~1.5 payment methods per customer and return (method_id, customer_id) pairs."""
    rows = []
    for cid, customer_ts in customer_data:
        n_methods = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        for i in range(n_methods):
            ptype, provider, brand = random.choice(_METHOD_TYPES)
            last4    = fake.numerify("####") if ptype in ("card", "bank_account") else None
            exp_m    = random.randint(1, 12)        if ptype == "card" else None
            exp_y    = random.randint(2025, 2030)   if ptype == "card" else None
            method_ts = _random_method_ts(customer_ts)
            rows.append((
                cid, ptype, provider, brand,
                last4, exp_m, exp_y,
                i == 0,      # first method is default
                method_ts,
            ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO payment_methods
              (customer_id, type, provider, card_brand, last4,
               expiry_month, expiry_year, is_default, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT method_id, customer_id FROM payment_methods")
        return [(str(r[0]), str(r[1])) for r in cur.fetchall()]


def seed_all(conn) -> dict:
    """Run full seed and return data pools for the payment generator."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM merchants")
        already = cur.fetchone()[0]

    if already > 0:
        print(f"SEED: static pool already present ({already} merchants) — skipping")
        with conn.cursor() as cur:
            cur.execute("SELECT merchant_id, created_at FROM merchants")
            merchant_data = [
                (str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()
            ]
            cur.execute("SELECT customer_id, created_at FROM customers")
            customer_data = [
                (str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()
            ]
            cur.execute("SELECT method_id, customer_id FROM payment_methods")
            method_rows = [(str(r[0]), str(r[1])) for r in cur.fetchall()]
        return {
            "merchant_data": merchant_data,
            "customer_data": customer_data,
            "method_rows":   method_rows,
        }

    print("SEED: inserting static pool (merchants, customers, payment_methods)...")
    merchant_data = seed_merchants(conn)
    customer_data = seed_customers(conn)
    method_rows   = seed_payment_methods(conn, customer_data)
    print(
        f"SEED: done — {len(merchant_data)} merchants, "
        f"{len(customer_data)} customers, {len(method_rows)} payment methods"
    )
    return {
        "merchant_data": merchant_data,
        "customer_data": customer_data,
        "method_rows":   method_rows,
    }
