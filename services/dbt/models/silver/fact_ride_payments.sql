-- Grain: one row per ride payment.
with payments as (
    select * from {{ ref('stg_rideon__ride_payments') }}
),
rides as (
    select ride_id, driver_id, city_id, category_id
    from {{ ref('stg_rideon__rides') }}
)
select
    p.payment_id,
    cast(p.created_at as date) as payment_date,
    p.ride_id,
    p.rider_id,
    r.driver_id,
    r.city_id,
    r.category_id,
    p.method,
    p.amount,
    p.currency,
    p.status,
    (p.status = 'captured') as is_captured,
    (p.status = 'refunded') as is_refunded,
    p.created_at,
    p.updated_at
from payments p
left join rides r on p.ride_id = r.ride_id
