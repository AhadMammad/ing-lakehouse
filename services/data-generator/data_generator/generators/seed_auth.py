"""Idempotent seeding of the Auth static pool.

Creates the RBAC reference data (5 roles, ~25 permissions, role→permission
matrix) and an initial cohort of ~2,000 users with bcrypt credentials,
profiles, and a primary role. All inserts use ON CONFLICT DO NOTHING so
re-runs are safe; the IDs and created_at timestamps are read back from the
DB so the event generator can filter by account age.

Passwords are hashed by Postgres' pgcrypto (`crypt(pw, gen_salt('bf'))`),
producing real bcrypt `$2a$` hashes without a Python bcrypt dependency.

A distinct Faker seed (4242) keeps this data independent of the payments
(default) and rideon (1337) generator pools.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

from data_generator.db import bulk_insert

fake = Faker()
Faker.seed(4242)
random.seed(4242)

_UTC = timezone.utc
_NOW = datetime.now(_UTC)

_INITIAL_USERS = 2000

# (name, description)
_ROLES = [
    ("admin",   "Full administrative access"),
    ("manager", "Team and billing management"),
    ("user",    "Standard end-user access"),
    ("auditor", "Read-only audit and reporting access"),
    ("support", "Customer-support operations"),
]

# (code, description)
_PERMISSIONS = [
    ("user:read",       "View user accounts"),
    ("user:write",      "Create/update user accounts"),
    ("user:delete",     "Delete user accounts"),
    ("role:read",       "View roles"),
    ("role:assign",     "Assign roles to users"),
    ("permission:read", "View permissions"),
    ("billing:read",    "View billing data"),
    ("billing:manage",  "Manage billing and subscriptions"),
    ("audit:read",      "View audit logs"),
    ("session:read",    "View active sessions"),
    ("session:revoke",  "Revoke active sessions"),
    ("mfa:manage",      "Manage MFA devices"),
    ("oauth:manage",    "Manage federated identity links"),
    ("settings:read",   "View settings"),
    ("settings:write",  "Update settings"),
    ("report:read",     "View reports"),
    ("report:export",   "Export reports"),
    ("apikey:read",     "View API keys"),
    ("apikey:manage",   "Manage API keys"),
    ("webhook:manage",  "Manage webhooks"),
    ("org:read",        "View organisation"),
    ("org:manage",      "Manage organisation"),
    ("notification:send", "Send notifications"),
    ("export:pii",      "Export personally identifiable information"),
    ("system:admin",    "System administration"),
]

# role name → permission codes (admin gets everything)
_ROLE_PERMISSIONS = {
    "manager": [
        "user:read", "user:write", "role:read", "role:assign", "billing:read",
        "billing:manage", "report:read", "report:export", "session:read",
        "session:revoke", "org:read", "settings:read", "settings:write",
    ],
    "user": ["user:read", "settings:read", "settings:write", "mfa:manage", "session:read"],
    "auditor": ["audit:read", "report:read", "user:read", "session:read", "org:read"],
    "support": ["user:read", "session:read", "session:revoke", "mfa:manage", "oauth:manage"],
}

# Primary-role distribution across the user base.
_ROLE_DIST_NAMES   = ["user", "support", "manager", "auditor", "admin"]
_ROLE_DIST_WEIGHTS = [88, 5, 4, 2, 1]

_STATUS_NAMES   = ["active", "locked", "disabled", "pending"]
_STATUS_WEIGHTS = [88, 5, 4, 3]

_COUNTRIES = ["US", "GB", "DE", "FR", "ES", "PL", "EE", "AZ", "GE", "TR", "NL", "SE"]
_LOCALES   = ["en_US", "en_GB", "de_DE", "fr_FR", "es_ES", "pl_PL", "az_AZ", "tr_TR"]
_TIMEZONES = [
    "America/New_York", "Europe/London", "Europe/Berlin", "Europe/Paris",
    "Europe/Warsaw", "Europe/Tallinn", "Asia/Baku", "Asia/Tbilisi",
]


def _random_signup_ts() -> datetime:
    """Initial cohort signed up 1 month–3 years ago."""
    days_ago = random.randint(30, 3 * 365)
    return _NOW - timedelta(days=days_ago, seconds=random.randint(0, 86399))


# ── RBAC reference data ───────────────────────────────────────────

def seed_roles(conn) -> dict[str, str]:
    rows = [(str(uuid.uuid4()), name, desc) for name, desc in _ROLES]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO roles (role_id, name, description) VALUES (%s, %s, %s) "
            "ON CONFLICT (name) DO NOTHING",
            rows,
        )
        conn.commit()
        cur.execute("SELECT name, role_id FROM roles")
        return {r[0]: str(r[1]) for r in cur.fetchall()}


def seed_permissions(conn) -> dict[str, str]:
    rows = [(str(uuid.uuid4()), code, desc) for code, desc in _PERMISSIONS]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO permissions (permission_id, code, description) VALUES (%s, %s, %s) "
            "ON CONFLICT (code) DO NOTHING",
            rows,
        )
        conn.commit()
        cur.execute("SELECT code, permission_id FROM permissions")
        return {r[0]: str(r[1]) for r in cur.fetchall()}


def seed_role_permissions(conn, role_ids: dict[str, str], perm_ids: dict[str, str]) -> None:
    rows: list[tuple[str, str]] = []
    # admin → every permission
    for code in perm_ids:
        rows.append((role_ids["admin"], perm_ids[code]))
    for role_name, codes in _ROLE_PERMISSIONS.items():
        for code in codes:
            if code in perm_ids:
                rows.append((role_ids[role_name], perm_ids[code]))
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            rows,
        )
        conn.commit()


# ── Initial user cohort ───────────────────────────────────────────

def seed_users(conn, role_ids: dict[str, str]) -> int:
    """Insert the initial user cohort with credentials, profiles, and a primary role."""
    existing = 0
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users")
        existing = cur.fetchone()[0]
    if existing >= _INITIAL_USERS:
        return existing

    user_rows, cred_rows, profile_rows, role_rows, audit_rows = [], [], [], [], []
    for _ in range(_INITIAL_USERS - existing):
        uid = str(uuid.uuid4())
        created = _random_signup_ts()
        status = random.choices(_STATUS_NAMES, weights=_STATUS_WEIGHTS)[0]
        primary_role = random.choices(_ROLE_DIST_NAMES, weights=_ROLE_DIST_WEIGHTS)[0]
        username = f"{fake.user_name()[:20]}_{uid[:6]}"
        email = f"{username}@{fake.free_email_domain()}"
        country = random.choice(_COUNTRIES)
        # Some users have already logged in since signup.
        last_login = None
        if status == "active" and random.random() < 0.8:
            span = int((_NOW - created).total_seconds())
            last_login = created + timedelta(seconds=random.randint(0, max(span, 1)))
        updated = last_login or created

        user_rows.append((
            uid, username, email, random.random() < 0.9, status,
            fake.name()[:80], country, created, updated, last_login,
        ))
        cred_rows.append((uid, fake.password(length=12), created, False))
        profile_rows.append((
            uid, fake.name()[:80], fake.phone_number()[:30],
            random.choice(_LOCALES), random.choice(_TIMEZONES),
            None, fake.date_of_birth(minimum_age=18, maximum_age=75), created,
        ))
        role_rows.append((uid, role_ids[primary_role], created))
        audit_rows.append((uid, "account_created", created, None, None))

    bulk_insert(conn, "users", user_rows, [
        "user_id", "username", "email", "email_verified", "status",
        "display_name", "country", "created_at", "updated_at", "last_login_at",
    ])
    # credentials: hash plaintext with pgcrypto bcrypt
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO credentials (user_id, password_hash, algo, password_changed_at, must_reset) "
            "VALUES (%s, crypt(%s, gen_salt('bf')), 'bcrypt', %s, %s) "
            "ON CONFLICT (user_id) DO NOTHING",
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
    return existing + len(user_rows)


# ── Pool readback ─────────────────────────────────────────────────

def _read_pools(conn) -> dict:
    """Read back the static pools from the DB (idempotent re-run path)."""
    with conn.cursor() as cur:
        cur.execute("SELECT name, role_id FROM roles")
        role_ids = {r[0]: str(r[1]) for r in cur.fetchall()}
        cur.execute(
            "SELECT u.user_id, u.created_at, u.status, u.country, "
            "       COALESCE(r.name, 'user') "
            "FROM users u "
            "LEFT JOIN LATERAL ("
            "  SELECT ro.name FROM user_roles ur "
            "  JOIN roles ro ON ro.role_id = ur.role_id "
            "  WHERE ur.user_id = u.user_id ORDER BY ur.granted_at LIMIT 1"
            ") r ON true"
        )
        users = [
            {
                "user_id": str(row[0]),
                "created_at": row[1].replace(tzinfo=_UTC) if row[1].tzinfo is None else row[1],
                "status": row[2],
                "country": row[3],
                "role": row[4],
            }
            for row in cur.fetchall()
        ]
    return {"role_ids": role_ids, "users": users}


def seed_all(conn) -> dict:
    """Run the full auth seed and return pools for the event generator.

    Pools:
      role_ids: {role_name: role_id}
      users:    [{user_id, created_at, status, country, role}]  (mutable — grows with signups)
    """
    print("SEED[auth]: ensuring RBAC reference data + user cohort...")
    role_ids = seed_roles(conn)
    perm_ids = seed_permissions(conn)
    seed_role_permissions(conn, role_ids, perm_ids)
    seed_users(conn, role_ids)
    pools = _read_pools(conn)
    print(
        f"SEED[auth]: ready — {len(role_ids)} roles, {len(perm_ids)} permissions, "
        f"{len(pools['users'])} users"
    )
    return pools
