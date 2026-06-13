"""Silver-layer specs.

Each silver table is a cleaned, deduplicated (latest ingest_ts per natural
key) projection of a bronze table, upserted (SCD1) on its natural key. The
actual cleaning SQL lives in jobs/silver.py; this module declares the table
set, source bronze table, natural key, and business columns used for the
silver _record_hash.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverSpec:
    name: str          # silver table name
    bronze: str        # source bronze table name
    keys: list[str]    # natural key(s)
    business_cols: list[str]  # columns hashed into _record_hash


SPECS: dict[str, SilverSpec] = {
    "users": SilverSpec(
        name="users", bronze="users_raw", keys=["user_id"],
        business_cols=["username", "email", "status", "country", "display_name", "email_verified"],
    ),
    "roles": SilverSpec(
        name="roles", bronze="roles_raw", keys=["role_id"],
        business_cols=["name", "description"],
    ),
    "permissions": SilverSpec(
        name="permissions", bronze="permissions_raw", keys=["permission_id"],
        business_cols=["code", "description"],
    ),
    "sessions": SilverSpec(
        name="sessions", bronze="sessions_raw", keys=["session_id"],
        business_cols=["user_id", "auth_method", "is_active", "was_revoked"],
    ),
    "login_attempts": SilverSpec(
        name="login_attempts", bronze="login_attempts_raw", keys=["attempt_id"],
        business_cols=["user_id", "success", "failure_reason", "auth_method", "mfa_challenged"],
    ),
    "oauth_accounts": SilverSpec(
        name="oauth_accounts", bronze="oauth_accounts_raw", keys=["oauth_account_id"],
        business_cols=["user_id", "provider"],
    ),
    "mfa_devices": SilverSpec(
        name="mfa_devices", bronze="mfa_devices_raw", keys=["mfa_device_id"],
        business_cols=["user_id", "device_type", "confirmed"],
    ),
}
