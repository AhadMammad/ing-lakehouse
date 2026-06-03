with src as (
    select * from {{ source('bronze', 'rideon_rides_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by ride_id order by ingest_ts desc) as _rn
    from src
)
select
    ride_id,
    rider_id,
    driver_id,
    vehicle_id,
    city_id,
    category_id,
    status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    pickup_lat,
    pickup_lng,
    dropoff_lat,
    dropoff_lng,
    distance_km,
    duration_min,
    surge_multiplier,
    cancelled_by,
    created_at,
    updated_at
from deduped
where _rn = 1
