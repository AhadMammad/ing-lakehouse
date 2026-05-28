-- ─────────────────────────────────────────────────────────────────
--  Payments platform monolith schema
--  Executed once by postgres on first container start.
-- ─────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Static reference tables ───────────────────────────────────────

CREATE TABLE merchants (
    merchant_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT        NOT NULL,
    category_code TEXT        NOT NULL,    -- RETAIL | FOOD | TRAVEL | ENTERTAINMENT | UTILITIES
    mcc_code      CHAR(4)     NOT NULL,    -- ISO 18245 merchant category code
    country       CHAR(2)     NOT NULL,
    city          TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'active',  -- active | suspended | closed
    created_at    TIMESTAMPTZ NOT NULL     -- set explicitly by generator (1–5 years ago)
);

CREATE TABLE customers (
    customer_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name    TEXT        NOT NULL,
    last_name     TEXT        NOT NULL,
    email         TEXT        NOT NULL UNIQUE,
    birthdate     DATE        NOT NULL,    -- age 18–100 years at time of seeding
    phone         TEXT,
    country       CHAR(2)     NOT NULL,
    city          TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL     -- set explicitly by generator (3 months–3 years ago)
);

CREATE TABLE payment_methods (
    method_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID        NOT NULL REFERENCES customers(customer_id),
    type          TEXT        NOT NULL,    -- card | bank_account | wallet
    provider      TEXT        NOT NULL,    -- Visa, Mastercard, IBAN, PayPal, etc.
    card_brand    TEXT,                    -- visa | mastercard | amex | null for non-card
    last4         CHAR(4),                 -- last 4 digits of card or account number
    expiry_month  SMALLINT,
    expiry_year   SMALLINT,
    is_default    BOOLEAN     NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Transactional tables (populated per-day by data-generator) ────

CREATE TABLE payments (
    payment_id    UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id   UUID           NOT NULL REFERENCES merchants(merchant_id),
    customer_id   UUID           NOT NULL REFERENCES customers(customer_id),
    method_id     UUID           NOT NULL REFERENCES payment_methods(method_id),
    amount        NUMERIC(14, 2) NOT NULL,
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    status        TEXT           NOT NULL,   -- pending | completed | failed | refunded
    error_code    TEXT,                       -- null on success
    description   TEXT,
    reference_id  TEXT,                       -- external idempotency key
    created_at    TIMESTAMPTZ    NOT NULL,
    updated_at    TIMESTAMPTZ    NOT NULL,
    settled_at    TIMESTAMPTZ
);

CREATE TABLE refunds (
    refund_id     UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id    UUID           NOT NULL REFERENCES payments(payment_id),
    amount        NUMERIC(14, 2) NOT NULL,
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    reason        TEXT           NOT NULL,   -- duplicate | fraudulent | requested_by_customer | other
    status        TEXT           NOT NULL DEFAULT 'pending',  -- pending | completed | failed
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE fees (
    fee_id        UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id    UUID           NOT NULL REFERENCES payments(payment_id),
    fee_type      TEXT           NOT NULL,   -- processing | interchange | scheme | fx
    amount        NUMERIC(14, 2) NOT NULL,
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE settlements (
    settlement_id   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id     UUID           NOT NULL REFERENCES merchants(merchant_id),
    settlement_date DATE           NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    currency        CHAR(3)        NOT NULL DEFAULT 'USD',
    payments_count  INTEGER        NOT NULL DEFAULT 0,
    status          TEXT           NOT NULL DEFAULT 'pending',  -- pending | processed | failed
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ── Indexes for ETL batch read patterns ───────────────────────────

CREATE INDEX ON payments (created_at);
CREATE INDEX ON payments (merchant_id, created_at);
CREATE INDEX ON payments (customer_id, created_at);
CREATE INDEX ON payments (status);
CREATE INDEX ON refunds (payment_id);
CREATE INDEX ON fees (payment_id);
CREATE INDEX ON settlements (merchant_id, settlement_date);
