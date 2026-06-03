with src as (
    select * from {{ source('bronze', 'rideon_cities_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by city_id order by ingest_ts desc) as _rn
    from src
)
select
    city_id,
    name        as city_name,
    country,
    timezone,
    launched_at
from deduped
where _rn = 1
