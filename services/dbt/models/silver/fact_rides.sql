-- Grain: one row per ride. FKs to all dims + ride measures and a fare rollup.
with rides as (
    select * from {{ ref('stg_rideon__rides') }}
),
fares as (
    select
        ride_id,
        total_fare,
        surge_amount,
        discount,
        currency
    from {{ ref('stg_rideon__fares') }}
)
select
    r.ride_id,
    cast(r.requested_at as date)             as ride_date,
    r.rider_id,
    r.driver_id,
    r.vehicle_id,
    r.city_id,
    r.category_id,
    r.status,
    r.cancelled_by,
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.distance_km,
    r.duration_min,
    r.surge_multiplier,
    f.total_fare,
    f.surge_amount,
    f.discount,
    f.currency,
    -- derived measures
    date_diff('second', r.requested_at, r.accepted_at) / 60.0 as wait_min,
    (r.status = 'completed')                 as is_completed,
    (r.status = 'cancelled')                 as is_cancelled,
    (r.surge_multiplier > 1.0)               as is_surge
from rides r
left join fares f on r.ride_id = f.ride_id
