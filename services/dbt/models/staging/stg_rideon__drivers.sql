with src as (
    select * from {{ source('bronze', 'rideon_drivers_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by driver_id order by ingest_ts desc) as _rn
    from src
)
select
    driver_id,
    first_name,
    last_name,
    email,
    phone,
    city_id,
    license_number,
    status,
    rating,
    onboarded_at
from deduped
where _rn = 1
