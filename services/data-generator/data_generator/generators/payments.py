"""Per-day payment generation.

Generates payments, refunds, fees, and settlements for a single date.
Only merchants and customers that existed on `day` are eligible.
Payment timestamps are always after both the merchant and the customer
were created, ensuring referential and temporal consistency.
"""
from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone

from faker import Faker

from data_generator.db import bulk_insert

fake = Faker()

_UTC = timezone.utc

_STATUS_WEIGHTS = [
    ("completed", 90),
    ("failed",     7),
    ("pending",    3),
]
_STATUSES, _WEIGHTS = zip(*_STATUS_WEIGHTS)

_ERROR_CODES = [
    "insufficient_funds",
    "card_declined",
    "expired_card",
    "invalid_cvv",
    "do_not_honor",
]

_CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP", "AZN"]

_REFUND_REASONS = [
    "requested_by_customer",
    "duplicate",
    "fraudulent",
    "other",
]

_FEE_TYPES = [
    ("processing",   60),
    ("interchange",  30),
    ("scheme",       10),
]
_FEE_TYPE_NAMES, _FEE_TYPE_WEIGHTS = zip(*_FEE_TYPES)


def _rand_ts_after(day: date, earliest: datetime) -> datetime:
    """Random UTC timestamp within `day` that is not before `earliest`."""
    day_start = datetime(day.year, day.month, day.day, tzinfo=_UTC)
    day_end   = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_UTC)
    lower = max(day_start, earliest)
    if lower >= day_end:
        return day_end
    span = int((day_end - lower).total_seconds())
    return lower + timedelta(seconds=random.randint(0, span))


def generate_for_date(
    conn,
    day: date,
    rows_per_day: int,
    merchant_data: list[tuple[str, datetime]],   # (merchant_id, created_at)
    customer_data: list[tuple[str, datetime]],   # (customer_id, created_at)
    method_rows: list[tuple[str, str]],           # (method_id, customer_id)
) -> None:
    """Generate all transactional rows for `day` and bulk-insert them."""

    # Only use entities that existed on `day`
    eligible_merchants = [(mid, mts) for mid, mts in merchant_data if mts.date() <= day]
    eligible_customers = [(cid, cts) for cid, cts in customer_data if cts.date() <= day]

    if not eligible_merchants or not eligible_customers:
        print(f"  {day}: SKIP — no eligible merchants or customers yet")
        return

    # Build customer → methods lookup
    customer_methods: dict[str, list[str]] = {}
    customer_ts_map: dict[str, datetime]   = {}
    for cid, cts in eligible_customers:
        customer_ts_map[cid] = cts

    for method_id, customer_id in method_rows:
        if customer_id in customer_ts_map:
            customer_methods.setdefault(customer_id, []).append(method_id)

    # Customers with at least one payment method
    eligible_cids = [cid for cid in customer_ts_map if cid in customer_methods]
    if not eligible_cids:
        print(f"  {day}: SKIP — no eligible customers with payment methods yet")
        return

    merchant_ts_map: dict[str, datetime] = {mid: mts for mid, mts in eligible_merchants}

    # ── Payments ─────────────────────────────────────────────────
    payment_rows = []
    payment_meta = []  # (payment_id, merchant_id, amount, currency, status)

    for _ in range(rows_per_day):
        pid       = str(uuid.uuid4())
        mid, mts  = random.choice(eligible_merchants)
        cid       = random.choice(eligible_cids)
        method_id = random.choice(customer_methods[cid])
        amount    = round(random.uniform(1.0, 4999.99), 2)
        currency  = random.choice(_CURRENCIES)
        status    = random.choices(_STATUSES, weights=_WEIGHTS)[0]
        error     = random.choice(_ERROR_CODES) if status == "failed" else None

        # Timestamp must be after both merchant and customer existed
        earliest  = max(mts, customer_ts_map[cid])
        ts        = _rand_ts_after(day, earliest)

        payment_rows.append((
            pid, mid, cid, method_id,
            amount, currency, status, error,
            fake.sentence(nb_words=4),
            str(uuid.uuid4()),              # reference_id
            ts, ts,                         # created_at, updated_at
            ts if status == "completed" else None,
        ))
        payment_meta.append((pid, mid, amount, currency, status))

    bulk_insert(conn, "payments", payment_rows, [
        "payment_id", "merchant_id", "customer_id", "method_id",
        "amount", "currency", "status", "error_code",
        "description", "reference_id",
        "created_at", "updated_at", "settled_at",
    ])

    # ── Refunds (~5% of completed payments) ──────────────────────
    completed = [(pid, amt, cur) for pid, _, amt, cur, st in payment_meta if st == "completed"]
    refund_sample = random.sample(completed, max(0, int(len(completed) * 0.05)))

    refund_rows = []
    for pid, amt, cur in refund_sample:
        refund_rows.append((
            str(uuid.uuid4()), pid,
            round(random.uniform(1.0, float(amt)), 2), cur,
            random.choice(_REFUND_REASONS),
            "completed",
        ))

    bulk_insert(conn, "refunds", refund_rows, [
        "refund_id", "payment_id", "amount", "currency", "reason", "status",
    ])

    # ── Fees (one per payment) ────────────────────────────────────
    _RATES = {"processing": 0.029, "interchange": 0.018, "scheme": 0.003}
    fee_rows = []
    for pid, _, amount, currency, _ in payment_meta:
        fee_type = random.choices(_FEE_TYPE_NAMES, weights=_FEE_TYPE_WEIGHTS)[0]
        fee_rows.append((
            str(uuid.uuid4()), pid,
            fee_type,
            round(float(amount) * _RATES[fee_type], 4),
            currency,
        ))

    bulk_insert(conn, "fees", fee_rows, [
        "fee_id", "payment_id", "fee_type", "amount", "currency",
    ])

    # ── Settlements (one per merchant with ≥1 completed payment) ─
    merchant_totals: dict[str, tuple[float, int, str]] = {}
    for _, mid, amt, cur, st in payment_meta:
        if st != "completed":
            continue
        total, count, c = merchant_totals.get(mid, (0.0, 0, cur))
        merchant_totals[mid] = (total + float(amt), count + 1, c)

    settlement_rows = []
    for mid, (total, count, cur) in merchant_totals.items():
        settlement_rows.append((
            str(uuid.uuid4()), mid,
            day, round(total, 2), cur,
            count, "processed",
        ))

    bulk_insert(conn, "settlements", settlement_rows, [
        "settlement_id", "merchant_id", "settlement_date",
        "amount", "currency", "payments_count", "status",
    ])

    print(
        f"  {day}: {len(payment_rows)} payments | "
        f"{len(refund_rows)} refunds | "
        f"{len(fee_rows)} fees | "
        f"{len(settlement_rows)} settlements"
    )
