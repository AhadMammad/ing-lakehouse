with src as (
    select * from {{ source('bronze', 'rideon_vehicle_categories_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by category_id order by ingest_ts desc) as _rn
    from src
)
select
    category_id,
    code,
    display_name,
    base_fare,
    per_km_rate,
    per_min_rate,
    min_fare
from deduped
where _rn = 1
