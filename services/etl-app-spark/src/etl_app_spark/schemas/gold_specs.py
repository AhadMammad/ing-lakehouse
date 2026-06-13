"""Gold-layer star-schema specs.

Dimensions:
  dim_user        — SCD Type 2 (one row per user version)
  dim_date        — calendar dimension
  dim_role        — SCD1
  dim_auth_method — SCD1
Facts:
  fact_login_attempts — grain: one login attempt
  fact_sessions       — grain: one session
"""
from __future__ import annotations

# Attributes whose change opens a new dim_user SCD2 version. Untracked columns
# (e.g. last_login_at) intentionally do NOT trigger a new version.
SCD2_TRACKED_ATTRS = [
    "username", "email", "status", "country", "display_name",
    "is_mfa_enabled", "primary_role",
]

GOLD_DIMS = ["dim_date", "dim_role", "dim_auth_method", "dim_user"]
GOLD_FACTS = ["fact_login_attempts", "fact_sessions"]

# auth_method → family, for dim_auth_method.
AUTH_METHOD_FAMILY = {
    "PASSWORD": "password",
    "OAUTH_GOOGLE": "oauth",
    "OAUTH_GITHUB": "oauth",
    "OAUTH_MICROSOFT": "oauth",
    "MFA_TOTP": "mfa",
    "MFA_SMS": "mfa",
    "MAGIC_LINK": "passwordless",
}
