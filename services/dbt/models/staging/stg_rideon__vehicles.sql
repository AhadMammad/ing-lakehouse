with src as (
    select * from {{ source('bronze', 'rideon_vehicles_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by vehicle_id order by ingest_ts desc) as _rn
    from src
)
select
    vehicle_id,
    driver_id,
    category_id,
    make,
    model,
    year,
    plate_number,
    color,
    registered_at
from deduped
where _rn = 1
