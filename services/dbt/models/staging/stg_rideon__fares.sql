with src as (
    select * from {{ source('bronze', 'rideon_fares_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by fare_id order by ingest_ts desc) as _rn
    from src
)
select
    fare_id,
    ride_id,
    base_fare,
    distance_fare,
    time_fare,
    surge_amount,
    discount,
    total_fare,
    currency,
    created_at
from deduped
where _rn = 1
