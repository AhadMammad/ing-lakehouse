-- ─────────────────────────────────────────────────────────────────
--  Auth — identity & access-management microservice schema (3rd source database)
--
--  Runs after 02_rideon.sql (alphabetical). docker-entrypoint-initdb.d
--  executes each file with psql against POSTGRES_DB (payments), so this
--  file CREATEs a separate `auth` database and \connects into it.
--
--  NOTE: init scripts run ONLY on a fresh data volume. Existing stacks
--  must `make clean` (destroys all source data) to pick up this DB.
--  CREATE DATABASE cannot run inside a transaction block — keep it as a
--  bare top-level statement (do not wrap this file in BEGIN/COMMIT).
-- ─────────────────────────────────────────────────────────────────

CREATE DATABASE auth;
\connect auth

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Identity core ─────────────────────────────────────────────────

CREATE TABLE users (
    user_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    username       TEXT        NOT NULL UNIQUE,
    email          TEXT        NOT NULL UNIQUE,
    email_verified BOOLEAN     NOT NULL DEFAULT false,
    status         TEXT        NOT NULL DEFAULT 'active',  -- active | locked | disabled | pending
    display_name   TEXT,
    country        CHAR(2),                                -- ISO-3166 alpha-2
    created_at     TIMESTAMPTZ NOT NULL,                   -- signup date (set by generator)
    updated_at     TIMESTAMPTZ NOT NULL,                   -- bumps on profile/status change → drives SCD2
    last_login_at  TIMESTAMPTZ
);

CREATE TABLE credentials (
    credential_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID        NOT NULL UNIQUE REFERENCES users(user_id),
    password_hash       TEXT        NOT NULL,              -- crypt(pw, gen_salt('bf')) — realistic bcrypt
    algo                TEXT        NOT NULL DEFAULT 'bcrypt',
    password_changed_at TIMESTAMPTZ NOT NULL,
    must_reset          BOOLEAN     NOT NULL DEFAULT false
);

CREATE TABLE user_profiles (
    user_id       UUID        PRIMARY KEY REFERENCES users(user_id),
    full_name     TEXT,
    phone         TEXT,
    locale        TEXT,                                    -- e.g. en_US, az_AZ
    timezone      TEXT,                                    -- IANA tz
    avatar_url    TEXT,
    date_of_birth DATE,
    updated_at    TIMESTAMPTZ NOT NULL
);

-- ── Authorization (RBAC) ──────────────────────────────────────────

CREATE TABLE roles (
    role_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,                      -- admin | manager | user | auditor | support
    description TEXT
);

CREATE TABLE permissions (
    permission_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code          TEXT NOT NULL UNIQUE,                    -- e.g. user:read, user:write, billing:manage
    description   TEXT
);

CREATE TABLE role_permissions (
    role_id       UUID NOT NULL REFERENCES roles(role_id),
    permission_id UUID NOT NULL REFERENCES permissions(permission_id),
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE user_roles (
    user_id    UUID        NOT NULL REFERENCES users(user_id),
    role_id    UUID        NOT NULL REFERENCES roles(role_id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, role_id)
);

-- ── Sessions & authentication events ──────────────────────────────

CREATE TABLE sessions (
    session_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL REFERENCES users(user_id),
    created_at  TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,
    ip_address  INET,
    user_agent  TEXT,
    auth_method TEXT        NOT NULL                        -- PASSWORD | OAUTH_GOOGLE | OAUTH_GITHUB | MFA_TOTP | MFA_SMS | MAGIC_LINK
);

CREATE TABLE login_attempts (
    attempt_id     BIGSERIAL   PRIMARY KEY,
    user_id        UUID        REFERENCES users(user_id),  -- nullable: unknown-username attempts
    username_tried TEXT        NOT NULL,
    attempted_at   TIMESTAMPTZ NOT NULL,
    success        BOOLEAN     NOT NULL,
    failure_reason TEXT,                                    -- bad_password | user_locked | unknown_user | mfa_failed | expired
    mfa_challenged BOOLEAN     NOT NULL DEFAULT false,
    ip_address     INET,
    user_agent     TEXT,
    auth_method    TEXT        NOT NULL
);

-- ── Federated identity, MFA, password resets ──────────────────────

CREATE TABLE oauth_accounts (
    oauth_account_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID        NOT NULL REFERENCES users(user_id),
    provider         TEXT        NOT NULL,                  -- google | github | microsoft
    provider_user_id TEXT        NOT NULL,
    linked_at        TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, provider_user_id)
);

CREATE TABLE mfa_devices (
    mfa_device_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID        NOT NULL REFERENCES users(user_id),
    device_type   TEXT        NOT NULL,                     -- totp | sms | webauthn
    label         TEXT,
    confirmed     BOOLEAN     NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL,
    last_used_at  TIMESTAMPTZ
);

CREATE TABLE password_reset_tokens (
    token_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID        NOT NULL REFERENCES users(user_id),
    token_hash TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);

-- ── Audit trail ───────────────────────────────────────────────────

CREATE TABLE audit_log (
    audit_id   BIGSERIAL   PRIMARY KEY,
    user_id    UUID        REFERENCES users(user_id),
    event_type TEXT        NOT NULL,                         -- login | logout | password_change | role_grant | mfa_enroll | account_locked | ...
    event_at   TIMESTAMPTZ NOT NULL,
    ip_address INET,
    detail     JSONB
);

-- ── Indexes for ETL batch read patterns ───────────────────────────

CREATE INDEX ON users (updated_at);
CREATE INDEX ON users (created_at);
CREATE INDEX ON credentials (password_changed_at);
CREATE INDEX ON user_roles (user_id);
CREATE INDEX ON sessions (user_id);
CREATE INDEX ON sessions (created_at);
CREATE INDEX ON login_attempts (attempted_at);
CREATE INDEX ON login_attempts (user_id, attempted_at);
CREATE INDEX ON oauth_accounts (user_id);
CREATE INDEX ON mfa_devices (user_id);
CREATE INDEX ON password_reset_tokens (created_at);
CREATE INDEX ON audit_log (event_at);
CREATE INDEX ON audit_log (user_id, event_at);
