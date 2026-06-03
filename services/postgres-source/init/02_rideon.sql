-- ─────────────────────────────────────────────────────────────────
--  Rideon — ride-hailing platform schema (2nd source database)
--
--  Runs after 01_schema.sql (alphabetical). docker-entrypoint-initdb.d
--  executes each file with psql against POSTGRES_DB (payments), so this
--  file CREATEs a separate `rideon` database and \connects into it.
--
--  NOTE: init scripts run ONLY on a fresh data volume. Existing stacks
--  must `make clean` (destroys all source data) to pick up this DB.
--  CREATE DATABASE cannot run inside a transaction block — keep it as a
--  bare top-level statement (do not wrap this file in BEGIN/COMMIT).
-- ─────────────────────────────────────────────────────────────────

CREATE DATABASE rideon;
\connect rideon

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Static reference tables ───────────────────────────────────────

CREATE TABLE cities (
    city_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT        NOT NULL,
    country      CHAR(2)     NOT NULL,
    timezone     TEXT        NOT NULL,    -- IANA tz, e.g. Europe/Tallinn
    launched_at  TIMESTAMPTZ NOT NULL,    -- when Rideon went live in this city
    UNIQUE (name, country)
);

CREATE TABLE vehicle_categories (
    category_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    code          TEXT        NOT NULL UNIQUE,  -- ECONOMY | COMFORT | XL | PREMIUM
    display_name  TEXT        NOT NULL,
    base_fare     NUMERIC(8, 2) NOT NULL,
    per_km_rate   NUMERIC(8, 2) NOT NULL,
    per_min_rate  NUMERIC(8, 2) NOT NULL,
    min_fare      NUMERIC(8, 2) NOT NULL
);

CREATE TABLE riders (
    rider_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name   TEXT        NOT NULL,
    last_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL UNIQUE,
    phone        TEXT,
    city_id      UUID        NOT NULL REFERENCES cities(city_id),
    rating       NUMERIC(2, 1) NOT NULL DEFAULT 5.0,  -- avg rating from drivers (1.0–5.0)
    created_at   TIMESTAMPTZ NOT NULL     -- set explicitly by generator (signup date)
);

CREATE TABLE drivers (
    driver_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name     TEXT        NOT NULL,
    last_name      TEXT        NOT NULL,
    email          TEXT        NOT NULL UNIQUE,
    phone          TEXT,
    city_id        UUID        NOT NULL REFERENCES cities(city_id),
    license_number TEXT        NOT NULL UNIQUE,
    status         TEXT        NOT NULL DEFAULT 'active',  -- active | suspended | offboarded
    rating         NUMERIC(2, 1) NOT NULL DEFAULT 5.0,     -- avg rating from riders (1.0–5.0)
    onboarded_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE vehicles (
    vehicle_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id     UUID        NOT NULL REFERENCES drivers(driver_id),
    category_id   UUID        NOT NULL REFERENCES vehicle_categories(category_id),
    make          TEXT        NOT NULL,
    model         TEXT        NOT NULL,
    year          SMALLINT    NOT NULL,
    plate_number  TEXT        NOT NULL UNIQUE,
    color         TEXT,
    registered_at TIMESTAMPTZ NOT NULL
);

-- ── Transactional tables (populated per-day by data-generator) ────

CREATE TABLE rides (
    ride_id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    rider_id         UUID          NOT NULL REFERENCES riders(rider_id),
    driver_id        UUID          NOT NULL REFERENCES drivers(driver_id),
    vehicle_id       UUID          NOT NULL REFERENCES vehicles(vehicle_id),
    city_id          UUID          NOT NULL REFERENCES cities(city_id),
    category_id      UUID          NOT NULL REFERENCES vehicle_categories(category_id),
    status           TEXT          NOT NULL,   -- requested | accepted | completed | cancelled
    requested_at     TIMESTAMPTZ   NOT NULL,
    accepted_at      TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    pickup_lat       NUMERIC(9, 6) NOT NULL,
    pickup_lng       NUMERIC(9, 6) NOT NULL,
    dropoff_lat      NUMERIC(9, 6),
    dropoff_lng      NUMERIC(9, 6),
    distance_km      NUMERIC(8, 2),
    duration_min     NUMERIC(8, 2),
    surge_multiplier NUMERIC(4, 2) NOT NULL DEFAULT 1.0,
    cancelled_by     TEXT,                      -- rider | driver | system | null
    created_at       TIMESTAMPTZ   NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL
);

CREATE TABLE fares (
    fare_id        UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id        UUID           NOT NULL REFERENCES rides(ride_id),
    base_fare      NUMERIC(10, 2) NOT NULL,
    distance_fare  NUMERIC(10, 2) NOT NULL,
    time_fare      NUMERIC(10, 2) NOT NULL,
    surge_amount   NUMERIC(10, 2) NOT NULL DEFAULT 0,
    discount       NUMERIC(10, 2) NOT NULL DEFAULT 0,
    total_fare     NUMERIC(10, 2) NOT NULL,
    currency       CHAR(3)        NOT NULL DEFAULT 'USD',
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE ride_payments (
    payment_id    UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id       UUID           NOT NULL REFERENCES rides(ride_id),
    rider_id      UUID           NOT NULL REFERENCES riders(rider_id),
    method        TEXT           NOT NULL,   -- card | cash | wallet
    amount        NUMERIC(10, 2) NOT NULL,
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    status        TEXT           NOT NULL,   -- captured | failed | refunded
    created_at    TIMESTAMPTZ    NOT NULL,
    updated_at    TIMESTAMPTZ    NOT NULL
);

CREATE TABLE ratings (
    rating_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id       UUID        NOT NULL REFERENCES rides(ride_id),
    rater_role    TEXT        NOT NULL,   -- rider | driver (who gave the rating)
    score         SMALLINT    NOT NULL,   -- 1..5
    comment       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE driver_payouts (
    payout_id     UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id     UUID           NOT NULL REFERENCES drivers(driver_id),
    payout_date   DATE           NOT NULL,
    gross_amount  NUMERIC(12, 2) NOT NULL,   -- total fares earned
    commission    NUMERIC(12, 2) NOT NULL,   -- platform cut
    net_amount    NUMERIC(12, 2) NOT NULL,   -- gross - commission
    currency      CHAR(3)        NOT NULL DEFAULT 'USD',
    rides_count   INTEGER        NOT NULL DEFAULT 0,
    status        TEXT           NOT NULL DEFAULT 'processed',  -- pending | processed | failed
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ── Indexes for ETL batch read patterns ───────────────────────────

CREATE INDEX ON rides (updated_at);
CREATE INDEX ON rides (city_id, requested_at);
CREATE INDEX ON rides (driver_id, requested_at);
CREATE INDEX ON rides (rider_id, requested_at);
CREATE INDEX ON rides (status);
CREATE INDEX ON fares (ride_id);
CREATE INDEX ON fares (created_at);
CREATE INDEX ON ride_payments (ride_id);
CREATE INDEX ON ride_payments (updated_at);
CREATE INDEX ON ratings (ride_id);
CREATE INDEX ON ratings (created_at);
CREATE INDEX ON driver_payouts (driver_id, payout_date);
