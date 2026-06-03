with src as (
    select * from {{ source('bronze', 'rideon_ratings_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by rating_id order by ingest_ts desc) as _rn
    from src
)
select
    rating_id,
    ride_id,
    rater_role,
    score,
    comment,
    created_at
from deduped
where _rn = 1
