"""Idempotent seeding of the Rideon static reference pool.

Creates ~10 cities, 4 vehicle categories, ~800 riders, ~300 drivers, and
~350 vehicles once. All inserts use ON CONFLICT DO NOTHING so re-runs are
safe. IDs and created_at timestamps are returned from the DB so the ride
generator can filter by entity age and ensure rides only occur after both
the rider and the driver existed.

A distinct Faker seed (1337) keeps this data independent of the payments
generator's pool.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()
Faker.seed(1337)
random.seed(1337)

_NOW = datetime.now(timezone.utc)

# (name, country, timezone) — Rideon's launch cities.
_CITIES = [
    ("Tallinn",   "EE", "Europe/Tallinn"),
    ("Riga",      "LV", "Europe/Riga"),
    ("Vilnius",   "LT", "Europe/Vilnius"),
    ("Warsaw",    "PL", "Europe/Warsaw"),
    ("Prague",    "CZ", "Europe/Prague"),
    ("Lisbon",    "PT", "Europe/Lisbon"),
    ("Bucharest", "RO", "Europe/Bucharest"),
    ("Baku",      "AZ", "Asia/Baku"),
    ("Tbilisi",   "GE", "Asia/Tbilisi"),
    ("Nairobi",   "KE", "Africa/Nairobi"),
]

# (code, display_name, base_fare, per_km_rate, per_min_rate, min_fare)
_CATEGORIES = [
    ("ECONOMY", "Rideon Go",      1.50, 0.65, 0.12, 2.50),
    ("COMFORT", "Rideon Comfort", 2.00, 0.90, 0.18, 3.50),
    ("XL",      "Rideon XL",      3.00, 1.20, 0.22, 5.00),
    ("PREMIUM", "Rideon Premium", 4.00, 1.60, 0.30, 7.00),
]

_VEHICLE_MAKES = [
    ("Toyota", ["Corolla", "Prius", "Camry", "RAV4"]),
    ("Volkswagen", ["Golf", "Passat", "Tiguan"]),
    ("Skoda", ["Octavia", "Superb", "Kodiaq"]),
    ("Hyundai", ["Elantra", "Tucson", "i30"]),
    ("Mercedes-Benz", ["E-Class", "V-Class", "GLC"]),
    ("BMW", ["3 Series", "5 Series", "X5"]),
]

_COLORS = ["black", "white", "silver", "grey", "blue", "red"]

_DRIVER_STATUSES = [("active", 90), ("suspended", 5), ("offboarded", 5)]
_DRIVER_STATUS_NAMES, _DRIVER_STATUS_WEIGHTS = zip(*_DRIVER_STATUSES)


# ── Timestamp helpers ─────────────────────────────────────────────

def _random_city_ts() -> datetime:
    """Cities launched 2–6 years ago."""
    days_ago = random.randint(2 * 365, 6 * 365)
    return _NOW - timedelta(days=days_ago, seconds=random.randint(0, 86399))


def _random_rider_ts() -> datetime:
    """Riders signed up 1 month–4 years ago."""
    days_ago = random.randint(30, 4 * 365)
    return _NOW - timedelta(days=days_ago, seconds=random.randint(0, 86399))


def _random_driver_ts() -> datetime:
    """Drivers onboarded 3 months–5 years ago."""
    days_ago = random.randint(90, 5 * 365)
    return _NOW - timedelta(days=days_ago, seconds=random.randint(0, 86399))


def _random_vehicle_ts(driver_ts: datetime) -> datetime:
    """Vehicle registered after the driver onboarded, within 1 year or now."""
    latest = min(_NOW, driver_ts + timedelta(days=365))
    if latest <= driver_ts:
        return driver_ts
    span = int((latest - driver_ts).total_seconds())
    return driver_ts + timedelta(seconds=random.randint(0, span))


# ── Seed functions ────────────────────────────────────────────────

def seed_cities(conn) -> list[str]:
    """Insert launch cities and return their city_ids."""
    rows = [(name, country, tz, _random_city_ts()) for name, country, tz in _CITIES]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO cities (name, country, timezone, launched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, country) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT city_id FROM cities")
        return [str(r[0]) for r in cur.fetchall()]


def seed_vehicle_categories(conn) -> list[str]:
    """Insert the four service tiers and return category_ids."""
    rows = list(_CATEGORIES)
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO vehicle_categories
              (code, display_name, base_fare, per_km_rate, per_min_rate, min_fare)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT category_id FROM vehicle_categories")
        return [str(r[0]) for r in cur.fetchall()]


def seed_riders(conn, city_ids: list[str]) -> list[tuple[str, datetime]]:
    """Insert 800 riders and return (rider_id, created_at) pairs."""
    rows = []
    emails_seen: set[str] = set()
    while len(rows) < 800:
        email = fake.unique.email()
        if email in emails_seen:
            continue
        emails_seen.add(email)
        rows.append((
            fake.first_name()[:80],
            fake.last_name()[:80],
            email,
            fake.phone_number()[:30],
            random.choice(city_ids),
            round(random.uniform(4.2, 5.0), 1),
            _random_rider_ts(),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO riders
              (first_name, last_name, email, phone, city_id, rating, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT rider_id, created_at FROM riders")
        return [(str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()]


def seed_drivers(conn, city_ids: list[str]) -> list[tuple[str, datetime]]:
    """Insert 300 drivers and return (driver_id, onboarded_at) pairs."""
    rows = []
    emails_seen: set[str] = set()
    licenses_seen: set[str] = set()
    while len(rows) < 300:
        email = fake.unique.email()
        if email in emails_seen:
            continue
        license_number = fake.unique.bothify("??######").upper()
        if license_number in licenses_seen:
            continue
        emails_seen.add(email)
        licenses_seen.add(license_number)
        rows.append((
            fake.first_name()[:80],
            fake.last_name()[:80],
            email,
            fake.phone_number()[:30],
            random.choice(city_ids),
            license_number,
            random.choices(_DRIVER_STATUS_NAMES, weights=_DRIVER_STATUS_WEIGHTS)[0],
            round(random.uniform(4.3, 5.0), 1),
            _random_driver_ts(),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO drivers
              (first_name, last_name, email, phone, city_id,
               license_number, status, rating, onboarded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT driver_id, onboarded_at FROM drivers")
        return [(str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1])
                for r in cur.fetchall()]


def seed_vehicles(
    conn,
    driver_data: list[tuple[str, datetime]],
    category_ids: list[str],
) -> list[tuple[str, str, datetime]]:
    """Insert ~1.15 vehicles per driver and return (vehicle_id, driver_id, registered_at)."""
    rows = []
    plates_seen: set[str] = set()
    for did, driver_ts in driver_data:
        n_vehicles = random.choices([1, 2], weights=[85, 15])[0]
        for _ in range(n_vehicles):
            make, models = random.choice(_VEHICLE_MAKES)
            plate = fake.unique.bothify("???-####").upper()
            if plate in plates_seen:
                continue
            plates_seen.add(plate)
            rows.append((
                did,
                random.choice(category_ids),
                make,
                random.choice(models),
                random.randint(2015, 2024),
                plate,
                random.choice(_COLORS),
                _random_vehicle_ts(driver_ts),
            ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO vehicles
              (driver_id, category_id, make, model, year,
               plate_number, color, registered_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (plate_number) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT vehicle_id, driver_id, registered_at FROM vehicles")
        return [(str(r[0]), str(r[1]),
                 r[2].replace(tzinfo=timezone.utc) if r[2].tzinfo is None else r[2])
                for r in cur.fetchall()]


def _read_pools(conn) -> dict:
    """Re-read the static pools from the DB (idempotent re-run path)."""
    with conn.cursor() as cur:
        cur.execute("SELECT city_id, country FROM cities")
        city_data = [(str(r[0]), r[1]) for r in cur.fetchall()]
        cur.execute(
            "SELECT category_id, code, base_fare, per_km_rate, per_min_rate, min_fare "
            "FROM vehicle_categories"
        )
        category_rows = [
            (str(r[0]), r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in cur.fetchall()
        ]
        cur.execute("SELECT rider_id, created_at, city_id FROM riders")
        rider_data = [
            (str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1], str(r[2]))
            for r in cur.fetchall()
        ]
        cur.execute("SELECT driver_id, onboarded_at, city_id FROM drivers")
        driver_data = [
            (str(r[0]), r[1].replace(tzinfo=timezone.utc) if r[1].tzinfo is None else r[1], str(r[2]))
            for r in cur.fetchall()
        ]
        cur.execute("SELECT vehicle_id, driver_id, category_id, registered_at FROM vehicles")
        vehicle_data = [
            (str(r[0]), str(r[1]), str(r[2]),
             r[3].replace(tzinfo=timezone.utc) if r[3].tzinfo is None else r[3])
            for r in cur.fetchall()
        ]
    return {
        "city_data":     city_data,
        "category_rows": category_rows,
        "rider_data":    rider_data,
        "driver_data":   driver_data,
        "vehicle_data":  vehicle_data,
    }


def seed_all(conn) -> dict:
    """Run full seed and return data pools for the ride generator.

    Pools (for downstream temporal/referential consistency):
      city_data:     [(city_id, country)]
      category_rows: [(category_id, code, base_fare, per_km_rate, per_min_rate, min_fare)]
      rider_data:    [(rider_id, created_at, city_id)]
      driver_data:   [(driver_id, onboarded_at, city_id)]
      vehicle_data:  [(vehicle_id, driver_id, category_id, registered_at)]
    """
    # Always run every seed step. Each insert is idempotent (ON CONFLICT DO
    # NOTHING) and the Faker seed is fixed, so the generated rows are stable
    # across runs and a partially-seeded pool (e.g. cities committed but a
    # later step crashed) self-heals instead of being locked in by a guard
    # that only checks one table.
    print("SEED[rideon]: ensuring static pool (cities, categories, riders, drivers, vehicles)...")
    city_ids = seed_cities(conn)
    category_ids = seed_vehicle_categories(conn)
    seed_riders(conn, city_ids)
    driver_pairs = seed_drivers(conn, city_ids)
    seed_vehicles(conn, driver_pairs, category_ids)
    pools = _read_pools(conn)
    print(
        f"SEED[rideon]: ready — {len(city_ids)} cities, {len(category_ids)} categories, "
        f"{len(pools['rider_data'])} riders, {len(pools['driver_data'])} drivers, "
        f"{len(pools['vehicle_data'])} vehicles"
    )
    return pools
