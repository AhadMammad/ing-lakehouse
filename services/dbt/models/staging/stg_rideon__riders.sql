with src as (
    select * from {{ source('bronze', 'rideon_riders_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by rider_id order by ingest_ts desc) as _rn
    from src
)
select
    rider_id,
    first_name,
    last_name,
    email,
    phone,
    city_id,
    rating,
    created_at
from deduped
where _rn = 1
