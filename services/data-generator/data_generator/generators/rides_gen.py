"""Per-day ride generation for Rideon.

Generates rides, fares, ride_payments, ratings, and driver_payouts for a
single date. Only riders and drivers that existed on `day` are eligible.
A ride is always pinned to a driver's city (rider and driver are matched on
the same city) and timestamps are after both the rider and driver existed,
ensuring referential and temporal consistency.
"""
from __future__ import annotations

import random
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from data_generator.db import bulk_insert

_UTC = timezone.utc

# Ride lifecycle outcome distribution.
_STATUS_WEIGHTS = [
    ("completed", 82),
    ("cancelled", 14),
    ("accepted",   2),   # accepted but not yet completed at snapshot
    ("requested",  2),   # awaiting a driver at snapshot
]
_STATUSES, _WEIGHTS = zip(*_STATUS_WEIGHTS)

_CANCELLED_BY = [("rider", 60), ("driver", 30), ("system", 10)]
_CANCELLED_BY_NAMES, _CANCELLED_BY_WEIGHTS = zip(*_CANCELLED_BY)

_PAYMENT_METHODS = [("card", 70), ("wallet", 20), ("cash", 10)]
_PAYMENT_METHOD_NAMES, _PAYMENT_METHOD_WEIGHTS = zip(*_PAYMENT_METHODS)

# Currency per country (rideon settles in the local-ish currency).
_CURRENCY_BY_COUNTRY = {
    "EE": "EUR", "LV": "EUR", "LT": "EUR", "PT": "EUR",
    "PL": "PLN", "CZ": "CZK", "RO": "RON",
    "AZ": "AZN", "GE": "GEL", "KE": "KES",
}

_COMMISSION_RATE = 0.20  # platform takes 20% of gross fares

_RATING_COMMENTS = [
    None, None, None,  # most ratings have no comment
    "Great driver", "Smooth ride", "Clean car", "On time",
    "Friendly", "Took a long route", "Hard to find pickup",
]


def _rand_ts_after(day: date, earliest: datetime) -> datetime:
    """Random UTC timestamp within `day` that is not before `earliest`."""
    day_start = datetime(day.year, day.month, day.day, tzinfo=_UTC)
    day_end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_UTC)
    lower = max(day_start, earliest)
    if lower >= day_end:
        return day_end
    span = int((day_end - lower).total_seconds())
    return lower + timedelta(seconds=random.randint(0, span))


def generate_for_date(
    conn,
    day: date,
    rides_per_day: int,
    city_data: list[tuple[str, str]],            # (city_id, country)
    category_rows: list[tuple],                  # (category_id, code, base, per_km, per_min, min_fare)
    rider_data: list[tuple[str, datetime, str]], # (rider_id, created_at, city_id)
    driver_data: list[tuple[str, datetime, str]],# (driver_id, onboarded_at, city_id)
    vehicle_data: list[tuple[str, str, str, datetime]],  # (vehicle_id, driver_id, category_id, registered_at)
) -> None:
    """Generate all transactional rows for `day` and bulk-insert them."""

    # Pricing + currency lookups.
    pricing = {c[0]: c for c in category_rows}
    city_country: dict[str, str] = {cid: country for cid, country in city_data}  # for currency

    # Eligible entities (existed on `day`).
    eligible_riders = [(rid, rts, cid) for rid, rts, cid in rider_data if rts.date() <= day]
    eligible_drivers = [(did, dts, cid) for did, dts, cid in driver_data if dts.date() <= day]
    if not eligible_riders or not eligible_drivers:
        print(f"  {day}: SKIP — no eligible riders or drivers yet")
        return

    # Vehicles eligible on `day`, grouped by driver.
    driver_vehicles: dict[str, list[tuple[str, str]]] = defaultdict(list)  # driver_id → [(vehicle_id, category_id)]
    for vid, did, cat_id, vts in vehicle_data:
        if vts.date() <= day:
            driver_vehicles[did].append((vid, cat_id))

    # Index riders/drivers by city so a ride matches local supply/demand.
    riders_by_city: dict[str, list[tuple[str, datetime]]] = defaultdict(list)
    for rid, rts, cid in eligible_riders:
        riders_by_city[cid].append((rid, rts))
    drivers_by_city: dict[str, list[tuple[str, datetime]]] = defaultdict(list)
    for did, dts, cid in eligible_drivers:
        if did in driver_vehicles:  # only drivers with a registered vehicle
            drivers_by_city[cid].append((did, dts))

    # Cities that have both riders and drivers with vehicles.
    active_cities = [c for c in riders_by_city if c in drivers_by_city]
    if not active_cities:
        print(f"  {day}: SKIP — no city has both riders and drivers with vehicles yet")
        return

    # ── Rides ────────────────────────────────────────────────────
    ride_rows = []
    # (ride_id, driver_id, rider_id, city_id, category_id, status, total_fare, currency, completed)
    ride_meta = []

    for _ in range(rides_per_day):
        city_id = random.choice(active_cities)
        rid, rts = random.choice(riders_by_city[city_id])
        did, dts = random.choice(drivers_by_city[city_id])
        vid, cat_id = random.choice(driver_vehicles[did])
        cat = pricing[cat_id]
        currency = _CURRENCY_BY_COUNTRY.get(city_country.get(city_id, ""), "EUR")

        status = random.choices(_STATUSES, weights=_WEIGHTS)[0]
        earliest = max(rts, dts)
        requested_at = _rand_ts_after(day, earliest)
        surge = random.choices([1.0, 1.2, 1.5, 2.0], weights=[70, 18, 9, 3])[0]

        pickup_lat = round(random.uniform(-1.0, 60.0), 6)
        pickup_lng = round(random.uniform(-1.0, 60.0), 6)

        accepted_at = started_at = completed_at = None
        dropoff_lat = dropoff_lng = None
        distance_km = duration_min = None
        cancelled_by = None
        ride_id = str(uuid.uuid4())

        if status in ("accepted", "completed", "cancelled"):
            accepted_at = requested_at + timedelta(seconds=random.randint(30, 240))

        if status == "completed":
            started_at = accepted_at + timedelta(seconds=random.randint(60, 600))
            distance_km = round(random.uniform(0.8, 35.0), 2)
            duration_min = round(distance_km * random.uniform(1.8, 3.2) + random.uniform(1, 8), 2)
            completed_at = started_at + timedelta(minutes=float(duration_min))
            dropoff_lat = round(pickup_lat + random.uniform(-0.2, 0.2), 6)
            dropoff_lng = round(pickup_lng + random.uniform(-0.2, 0.2), 6)
        elif status == "cancelled":
            cancelled_by = random.choices(_CANCELLED_BY_NAMES, weights=_CANCELLED_BY_WEIGHTS)[0]

        updated_at = completed_at or accepted_at or requested_at

        # Fare (only meaningful for completed rides).
        total_fare = 0.0
        if status == "completed":
            base = cat[2]
            distance_fare = cat[3] * float(distance_km)
            time_fare = cat[4] * float(duration_min)
            subtotal = base + distance_fare + time_fare
            surged = subtotal * float(surge)
            total_fare = round(max(surged, cat[5]), 2)

        ride_rows.append((
            ride_id, rid, did, vid, city_id, cat_id, status,
            requested_at, accepted_at, started_at, completed_at,
            pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
            distance_km, duration_min, surge, cancelled_by,
            requested_at, updated_at,
        ))
        ride_meta.append((
            ride_id, did, rid, city_id, cat_id, status,
            total_fare, currency, requested_at, completed_at,
            distance_km, duration_min, surge, float(cat[2]),
        ))

    bulk_insert(conn, "rides", ride_rows, [
        "ride_id", "rider_id", "driver_id", "vehicle_id", "city_id", "category_id", "status",
        "requested_at", "accepted_at", "started_at", "completed_at",
        "pickup_lat", "pickup_lng", "dropoff_lat", "dropoff_lng",
        "distance_km", "duration_min", "surge_multiplier", "cancelled_by",
        "created_at", "updated_at",
    ])

    completed = [m for m in ride_meta if m[5] == "completed"]

    # ── Fares (1:1 with completed rides) ─────────────────────────
    fare_rows = []
    for (ride_id, _did, _rid, _city, cat_id, _st,
         total_fare, currency, _req, completed_at,
         distance_km, duration_min, surge, base) in completed:
        cat = pricing[cat_id]
        distance_fare = round(cat[3] * float(distance_km), 2)
        time_fare = round(cat[4] * float(duration_min), 2)
        subtotal = base + distance_fare + time_fare
        surge_amount = round(subtotal * (float(surge) - 1.0), 2)
        discount = round(total_fare * random.choice([0.0, 0.0, 0.0, 0.1, 0.15]), 2)
        fare_rows.append((
            str(uuid.uuid4()), ride_id,
            round(base, 2), distance_fare, time_fare,
            surge_amount, discount,
            round(max(total_fare - discount, 0.0), 2), currency,
            completed_at,   # created_at = ride completion time (on `day`)
        ))

    bulk_insert(conn, "fares", fare_rows, [
        "fare_id", "ride_id", "base_fare", "distance_fare", "time_fare",
        "surge_amount", "discount", "total_fare", "currency", "created_at",
    ])

    # ── Ride payments (1 per completed ride) ─────────────────────
    payment_rows = []
    for (ride_id, _did, rider_id, _city, _cat, _st,
         total_fare, currency, _req, completed_at, *_rest) in completed:
        method = random.choices(_PAYMENT_METHOD_NAMES, weights=_PAYMENT_METHOD_WEIGHTS)[0]
        status = random.choices(["captured", "failed", "refunded"], weights=[94, 4, 2])[0]
        payment_rows.append((
            str(uuid.uuid4()), ride_id, rider_id, method,
            total_fare, currency, status,
            completed_at, completed_at,
        ))

    bulk_insert(conn, "ride_payments", payment_rows, [
        "payment_id", "ride_id", "rider_id", "method", "amount",
        "currency", "status", "created_at", "updated_at",
    ])

    # ── Ratings (~70% of completed rides, 1–2 per ride) ──────────
    rating_rows = []
    for (ride_id, _did, _rid, _city, _cat, _st, _tot, _cur, _req, completed_at, *_rest) in completed:
        if random.random() > 0.70:
            continue
        roles = ["rider"] if random.random() < 0.5 else ["rider", "driver"]
        for role in roles:
            rating_rows.append((
                str(uuid.uuid4()), ride_id, role,
                random.choices([5, 4, 3, 2, 1], weights=[68, 18, 8, 4, 2])[0],
                random.choice(_RATING_COMMENTS),
                completed_at,
            ))

    bulk_insert(conn, "ratings", rating_rows, [
        "rating_id", "ride_id", "rater_role", "score", "comment", "created_at",
    ])

    # ── Driver payouts (1 per driver with ≥1 completed ride) ─────
    driver_totals: dict[str, tuple[float, int, str]] = {}
    for (_ride_id, did, _rid, _city, _cat, _st, total_fare, currency, *_rest) in completed:
        gross, count, cur = driver_totals.get(did, (0.0, 0, currency))
        driver_totals[did] = (gross + float(total_fare), count + 1, cur)

    payout_rows = []
    for did, (gross, count, currency) in driver_totals.items():
        gross = round(gross, 2)
        commission = round(gross * _COMMISSION_RATE, 2)
        payout_rows.append((
            str(uuid.uuid4()), did, day,
            gross, commission, round(gross - commission, 2),
            currency, count, "processed",
        ))

    bulk_insert(conn, "driver_payouts", payout_rows, [
        "payout_id", "driver_id", "payout_date",
        "gross_amount", "commission", "net_amount",
        "currency", "rides_count", "status",
    ])

    print(
        f"  {day}: {len(ride_rows)} rides | {len(fare_rows)} fares | "
        f"{len(payment_rows)} payments | {len(rating_rows)} ratings | "
        f"{len(payout_rows)} payouts"
    )
