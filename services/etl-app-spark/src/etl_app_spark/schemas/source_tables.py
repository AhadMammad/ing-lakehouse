"""Bronze source-table specs for the auth identity service.

Each spec declares how to read a Postgres `auth` table into bronze:
  * source       — Postgres table name
  * bronze       — bronze Iceberg table name (suffixed `_raw`)
  * mode         — "snapshot" (reference/dimension-like, full reload per
                   logical_date) or "incremental" (event tables, append the
                   day's slice filtered on `watermark`)
  * watermark    — timestamp column for the incremental date filter
  * columns      — explicit SELECT list; INET/JSONB are cast to text so the
                   Spark JDBC reader does not choke on PG-specific types.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeSpec:
    source: str
    bronze: str
    mode: str
    columns: list[str]
    watermark: str | None = None

    def select_sql(self, logical_date: str) -> str:
        cols = ", ".join(self.columns)
        if self.mode == "incremental":
            if not self.watermark:
                raise ValueError(f"incremental table {self.source} needs a watermark")
            where = f"WHERE {self.watermark}::date = DATE '{logical_date}'"
        else:
            where = ""
        # Wrapped subquery handed to Spark's JDBC `dbtable` option.
        return f"(SELECT {cols} FROM {self.source} {where}) AS t"


# INET → ::text, JSONB → ::text to keep the JDBC reader on safe types.
SPECS: dict[str, BronzeSpec] = {
    "users": BronzeSpec(
        source="users", bronze="users_raw", mode="snapshot",
        columns=[
            "user_id", "username", "email", "email_verified", "status",
            "display_name", "country", "created_at", "updated_at", "last_login_at",
        ],
    ),
    "credentials": BronzeSpec(
        source="credentials", bronze="credentials_raw", mode="snapshot",
        columns=[
            "credential_id", "user_id", "password_hash", "algo",
            "password_changed_at", "must_reset",
        ],
    ),
    "user_profiles": BronzeSpec(
        source="user_profiles", bronze="user_profiles_raw", mode="snapshot",
        columns=[
            "user_id", "full_name", "phone", "locale", "timezone",
            "avatar_url", "date_of_birth", "updated_at",
        ],
    ),
    "roles": BronzeSpec(
        source="roles", bronze="roles_raw", mode="snapshot",
        columns=["role_id", "name", "description"],
    ),
    "permissions": BronzeSpec(
        source="permissions", bronze="permissions_raw", mode="snapshot",
        columns=["permission_id", "code", "description"],
    ),
    "role_permissions": BronzeSpec(
        source="role_permissions", bronze="role_permissions_raw", mode="snapshot",
        columns=["role_id", "permission_id"],
    ),
    "user_roles": BronzeSpec(
        source="user_roles", bronze="user_roles_raw", mode="snapshot",
        columns=["user_id", "role_id", "granted_at"],
    ),
    "sessions": BronzeSpec(
        source="sessions", bronze="sessions_raw", mode="incremental", watermark="created_at",
        columns=[
            "session_id", "user_id", "created_at", "expires_at", "revoked_at",
            "ip_address::text AS ip_address", "user_agent", "auth_method",
        ],
    ),
    "login_attempts": BronzeSpec(
        source="login_attempts", bronze="login_attempts_raw", mode="incremental", watermark="attempted_at",
        columns=[
            "attempt_id", "user_id", "username_tried", "attempted_at", "success",
            "failure_reason", "mfa_challenged", "ip_address::text AS ip_address",
            "user_agent", "auth_method",
        ],
    ),
    "oauth_accounts": BronzeSpec(
        source="oauth_accounts", bronze="oauth_accounts_raw", mode="snapshot",
        columns=[
            "oauth_account_id", "user_id", "provider", "provider_user_id", "linked_at",
        ],
    ),
    "mfa_devices": BronzeSpec(
        source="mfa_devices", bronze="mfa_devices_raw", mode="snapshot",
        columns=[
            "mfa_device_id", "user_id", "device_type", "label",
            "confirmed", "created_at", "last_used_at",
        ],
    ),
    "password_reset_tokens": BronzeSpec(
        source="password_reset_tokens", bronze="password_reset_tokens_raw",
        mode="incremental", watermark="created_at",
        columns=["token_id", "user_id", "token_hash", "created_at", "expires_at", "used_at"],
    ),
    "audit_log": BronzeSpec(
        source="audit_log", bronze="audit_log_raw", mode="incremental", watermark="event_at",
        columns=[
            "audit_id", "user_id", "event_type", "event_at",
            "ip_address::text AS ip_address", "detail::text AS detail",
        ],
    ),
}
