"""Auth event generation — historical (per-day) and real-time (per-tick).

Both modes share the same primitives:
  * signups        → users (+ credentials, profiles, user_roles, audit)
  * sessions       → sessions (+ login audit)
  * login_attempts → login_attempts (mix of success / failure)
  * status changes → users.status flip + updated_at bump (DRIVES gold SCD2)
  * oauth / mfa / password resets / audit events

`generate_for_date` backfills a single calendar day; `generate_tick` emits a
small burst of events stamped "now" for the streaming simulation. Both mutate
the in-memory `pools["users"]` list so later days/ticks see new signups and
status changes.

A distinct Faker seed (909) keeps event data independent of the seed pool.
"""
from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone

from psycopg2.extras import Json

from faker import Faker

from data_generator.db import bulk_insert

fake = Faker()
Faker.seed(909)
random.seed(909)

_UTC = timezone.utc

_AUTH_METHODS = [
    ("PASSWORD", 70), ("OAUTH_GOOGLE", 12), ("OAUTH_GITHUB", 6),
    ("MFA_TOTP", 7), ("MFA_SMS", 3), ("MAGIC_LINK", 2),
]
_AM_NAMES, _AM_WEIGHTS = zip(*_AUTH_METHODS)

_FAIL_REASONS = [
    ("bad_password", 60), ("unknown_user", 18), ("user_locked", 10),
    ("mfa_failed", 8), ("expired", 4),
]
_FR_NAMES, _FR_WEIGHTS = zip(*_FAIL_REASONS)

_OAUTH_PROVIDERS = ["google", "github", "microsoft"]
_MFA_TYPES = [("totp", 60), ("sms", 30), ("webauthn", 10)]
_MFA_NAMES, _MFA_WEIGHTS = zip(*_MFA_TYPES)

_COUNTRIES = ["US", "GB", "DE", "FR", "ES", "PL", "EE", "AZ", "GE", "TR", "NL", "SE"]
_LOCALES   = ["en_US", "en_GB", "de_DE", "fr_FR", "es_ES", "pl_PL", "az_AZ", "tr_TR"]
_TIMEZONES = [
    "America/New_York", "Europe/London", "Europe/Berlin", "Europe/Paris",
    "Europe/Warsaw", "Europe/Tallinn", "Asia/Baku", "Asia/Tbilisi",
]
_ROLE_DIST_NAMES   = ["user", "support", "manager", "auditor", "admin"]
_ROLE_DIST_WEIGHTS = [90, 4, 3, 2, 1]


# ── Timestamp helpers ─────────────────────────────────────────────

def _rand_ts_within(day: date, earliest: datetime | None = None) -> datetime:
    """Random UTC timestamp within `day`, not before `earliest`."""
    day_start = datetime(day.year, day.month, day.day, tzinfo=_UTC)
    day_end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_UTC)
    lower = max(day_start, earliest) if earliest else day_start
    if lower >= day_end:
        return day_end
    span = int((day_end - lower).total_seconds())
    return lower + timedelta(seconds=random.randint(0, span))


def _auth_method() -> str:
    return random.choices(_AM_NAMES, weights=_AM_WEIGHTS)[0]


# ── Primitives (operate on a concrete timestamp) ──────────────────

def _signup_users(conn, role_ids: dict[str, str], timestamps: list[datetime], pools: dict) -> int:
    """Create new users (with credentials/profile/role/audit) at the given timestamps."""
    if not timestamps:
        return 0
    user_rows, cred_rows, profile_rows, role_rows, audit_rows = [], [], [], [], []
    for ts in timestamps:
        uid = str(uuid.uuid4())
        primary_role = random.choices(_ROLE_DIST_NAMES, weights=_ROLE_DIST_WEIGHTS)[0]
        username = f"{fake.user_name()[:20]}_{uid[:6]}"
        email = f"{username}@{fake.free_email_domain()}"
        country = random.choice(_COUNTRIES)
        status = "pending" if random.random() < 0.15 else "active"
        user_rows.append((
            uid, username, email, status == "active", status,
            fake.name()[:80], country, ts, ts, None,
        ))
        cred_rows.append((uid, fake.password(length=12), ts, False))
        profile_rows.append((
            uid, fake.name()[:80], fake.phone_number()[:30],
            random.choice(_LOCALES), random.choice(_TIMEZONES),
            None, fake.date_of_birth(minimum_age=18, maximum_age=75), ts,
        ))
        role_rows.append((uid, role_ids[primary_role], ts))
        audit_rows.append((uid, "account_created", ts, fake.ipv4_public(), None))
        pools["users"].append({
            "user_id": uid, "created_at": ts, "status": status,
            "country": country, "role": primary_role,
        })

    bulk_insert(conn, "users", user_rows, [
        "user_id", "username", "email", "email_verified", "status",
        "display_name", "country", "created_at", "updated_at", "last_login_at",
    ])
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO credentials (user_id, password_hash, algo, password_changed_at, must_reset) "
            "VALUES (%s, crypt(%s, gen_salt('bf')), 'bcrypt', %s, %s)",
            cred_rows,
        )
        conn.commit()
    bulk_insert(conn, "user_profiles", profile_rows, [
        "user_id", "full_name", "phone", "locale", "timezone",
        "avatar_url", "date_of_birth", "updated_at",
    ])
    bulk_insert(conn, "user_roles", role_rows, ["user_id", "role_id", "granted_at"])
    bulk_insert(conn, "audit_log", audit_rows, [
        "user_id", "event_type", "event_at", "ip_address", "detail",
    ])
    return len(user_rows)


def _gen_sessions(conn, eligible: list[dict], n: int, ts_func) -> int:
    """Create sessions for active users + a matching login audit event."""
    active = [u for u in eligible if u["status"] == "active"]
    if not active or n <= 0:
        return 0
    session_rows, audit_rows = [], []
    for _ in range(n):
        u = random.choice(active)
        ts = ts_func(u["created_at"])
        revoked = None
        if random.random() < 0.10:
            revoked = ts + timedelta(minutes=random.randint(5, 600))
        session_rows.append((
            str(uuid.uuid4()), u["user_id"], ts,
            ts + timedelta(hours=random.choice([2, 8, 24, 168])),
            revoked, fake.ipv4_public(), fake.user_agent(), _auth_method(),
        ))
        audit_rows.append((u["user_id"], "login", ts, session_rows[-1][5], None))
    bulk_insert(conn, "sessions", session_rows, [
        "session_id", "user_id", "created_at", "expires_at",
        "revoked_at", "ip_address", "user_agent", "auth_method",
    ])
    bulk_insert(conn, "audit_log", audit_rows, [
        "user_id", "event_type", "event_at", "ip_address", "detail",
    ])
    return len(session_rows)


def _gen_login_attempts(conn, eligible: list[dict], n: int, ts_func) -> int:
    """Create login attempts (~85% success), including some unknown-user failures."""
    if not eligible or n <= 0:
        return 0
    rows = []
    for _ in range(n):
        method = _auth_method()
        mfa_challenged = method in ("MFA_TOTP", "MFA_SMS") or random.random() < 0.1
        if random.random() < 0.07:
            # Unknown username — no user_id.
            rows.append((
                None, fake.user_name(), ts_func(None), False,
                "unknown_user", mfa_challenged, fake.ipv4_public(),
                fake.user_agent(), method,
            ))
            continue
        u = random.choice(eligible)
        ts = ts_func(u["created_at"])
        success = u["status"] == "active" and random.random() < 0.88
        reason = None
        if not success:
            if u["status"] == "locked":
                reason = "user_locked"
            elif u["status"] == "disabled":
                reason = "expired"
            else:
                reason = random.choices(_FR_NAMES, weights=_FR_WEIGHTS)[0]
        rows.append((
            u["user_id"], f"user_{u['user_id'][:6]}", ts, success,
            reason, mfa_challenged, fake.ipv4_public(), fake.user_agent(), method,
        ))
    bulk_insert(conn, "login_attempts", rows, [
        "user_id", "username_tried", "attempted_at", "success",
        "failure_reason", "mfa_challenged", "ip_address", "user_agent", "auth_method",
    ])
    return len(rows)


def _gen_status_changes(conn, eligible: list[dict], n: int, ts_func) -> int:
    """Flip user statuses (lock/disable/unlock/verify) — bumps updated_at to drive SCD2."""
    if not eligible or n <= 0:
        return 0
    sample = random.sample(eligible, min(n, len(eligible)))
    updates, audit_rows = [], []
    for u in sample:
        cur_status = u["status"]
        if cur_status == "active":
            new_status, event = random.choices(
                [("locked", "account_locked"), ("disabled", "account_disabled")],
                weights=[80, 20],
            )[0]
        elif cur_status == "locked":
            new_status, event = "active", "account_unlocked"
        elif cur_status == "pending":
            new_status, event = "active", "email_verified"
        else:  # disabled
            new_status, event = "active", "account_reactivated"
        ts = ts_func(u["created_at"])
        u["status"] = new_status
        updates.append((new_status, ts, u["user_id"]))
        audit_rows.append((u["user_id"], event, ts, fake.ipv4_public(),
                           Json({"from": cur_status, "to": new_status})))
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE users SET status = %s, updated_at = %s WHERE user_id = %s",
            updates,
        )
        conn.commit()
    bulk_insert(conn, "audit_log", audit_rows, [
        "user_id", "event_type", "event_at", "ip_address", "detail",
    ])
    return len(updates)


def _gen_oauth(conn, eligible: list[dict], n: int, ts_func) -> int:
    if not eligible or n <= 0:
        return 0
    rows = []
    for _ in range(n):
        u = random.choice(eligible)
        rows.append((
            str(uuid.uuid4()), u["user_id"], random.choice(_OAUTH_PROVIDERS),
            uuid.uuid4().hex, ts_func(u["created_at"]),
        ))
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO oauth_accounts (oauth_account_id, user_id, provider, provider_user_id, linked_at) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (provider, provider_user_id) DO NOTHING",
            rows,
        )
        conn.commit()
    return len(rows)


def _gen_mfa(conn, eligible: list[dict], n: int, ts_func) -> int:
    if not eligible or n <= 0:
        return 0
    rows = []
    for _ in range(n):
        u = random.choice(eligible)
        ts = ts_func(u["created_at"])
        device_type = random.choices(_MFA_NAMES, weights=_MFA_WEIGHTS)[0]
        rows.append((
            str(uuid.uuid4()), u["user_id"], device_type,
            f"{device_type}-{fake.word()}", True, ts, ts,
        ))
    bulk_insert(conn, "mfa_devices", rows, [
        "mfa_device_id", "user_id", "device_type", "label",
        "confirmed", "created_at", "last_used_at",
    ])
    return len(rows)


def _gen_resets(conn, eligible: list[dict], n: int, ts_func) -> int:
    if not eligible or n <= 0:
        return 0
    token_rows, audit_rows = [], []
    for _ in range(n):
        u = random.choice(eligible)
        ts = ts_func(u["created_at"])
        used = ts + timedelta(minutes=random.randint(2, 60)) if random.random() < 0.6 else None
        token_rows.append((
            str(uuid.uuid4()), u["user_id"], uuid.uuid4().hex,
            ts, ts + timedelta(hours=1), used,
        ))
        audit_rows.append((u["user_id"], "password_reset_requested", ts, fake.ipv4_public(), None))
    bulk_insert(conn, "password_reset_tokens", token_rows, [
        "token_id", "user_id", "token_hash", "created_at", "expires_at", "used_at",
    ])
    bulk_insert(conn, "audit_log", audit_rows, [
        "user_id", "event_type", "event_at", "ip_address", "detail",
    ])
    return len(token_rows)


# ── Public orchestrators ──────────────────────────────────────────

def generate_for_date(conn, day: date, users_per_day: int, pools: dict) -> None:
    """Generate a full day of auth activity and bulk-insert it."""
    role_ids = pools["role_ids"]

    # New signups for the day.
    signup_ts = [_rand_ts_within(day) for _ in range(users_per_day)]
    n_signups = _signup_users(conn, role_ids, signup_ts, pools)

    # Users that existed on `day`.
    eligible = [u for u in pools["users"] if u["created_at"].date() <= day]
    if not eligible:
        print(f"  {day}: {n_signups} signups | no eligible users for activity yet")
        return

    def ts_func(earliest):
        return _rand_ts_within(day, earliest)

    active = sum(1 for u in eligible if u["status"] == "active")
    n_sessions = max(1, int(active * 0.18))
    n_attempts = max(1, int(active * 0.25))
    n_status   = max(0, int(len(eligible) * 0.01))
    n_oauth    = max(0, int(users_per_day * 0.3))
    n_mfa      = max(0, int(users_per_day * 0.2))
    n_resets   = max(0, int(active * 0.01))

    s = _gen_sessions(conn, eligible, n_sessions, ts_func)
    a = _gen_login_attempts(conn, eligible, n_attempts, ts_func)
    c = _gen_status_changes(conn, eligible, n_status, ts_func)
    o = _gen_oauth(conn, eligible, n_oauth, ts_func)
    m = _gen_mfa(conn, eligible, n_mfa, ts_func)
    r = _gen_resets(conn, eligible, n_resets, ts_func)

    print(
        f"  {day}: {n_signups} signups | {s} sessions | {a} logins | "
        f"{c} status-changes | {o} oauth | {m} mfa | {r} resets"
    )


def generate_tick(conn, pools: dict, rate: int, now: datetime) -> dict:
    """Emit a small real-time burst of events stamped around `now`."""
    role_ids = pools["role_ids"]

    def ts_func(_earliest=None):
        # Jitter a few seconds around `now`.
        return now - timedelta(seconds=random.randint(0, 5))

    counts = {"signups": 0, "sessions": 0, "logins": 0, "status": 0,
              "oauth": 0, "mfa": 0, "resets": 0}

    # Occasional signup.
    if random.random() < 0.2:
        counts["signups"] = _signup_users(conn, role_ids, [ts_func()], pools)

    eligible = pools["users"]
    counts["logins"]   = _gen_login_attempts(conn, eligible, rate, ts_func)
    counts["sessions"] = _gen_sessions(conn, eligible, max(1, rate // 2), ts_func)
    if random.random() < 0.15:
        counts["status"] = _gen_status_changes(conn, eligible, 1, ts_func)
    if random.random() < 0.10:
        counts["oauth"] = _gen_oauth(conn, eligible, 1, ts_func)
    if random.random() < 0.10:
        counts["mfa"] = _gen_mfa(conn, eligible, 1, ts_func)
    if random.random() < 0.08:
        counts["resets"] = _gen_resets(conn, eligible, 1, ts_func)
    return counts
