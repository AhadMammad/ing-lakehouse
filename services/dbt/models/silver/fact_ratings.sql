-- Grain: one row per rating event.
with ratings as (
    select * from {{ ref('stg_rideon__ratings') }}
),
rides as (
    select ride_id, rider_id, driver_id, city_id, category_id
    from {{ ref('stg_rideon__rides') }}
)
select
    rt.rating_id,
    cast(rt.created_at as date) as rating_date,
    rt.ride_id,
    r.rider_id,
    r.driver_id,
    r.city_id,
    r.category_id,
    rt.rater_role,
    rt.score,
    rt.comment,
    rt.created_at
from ratings rt
left join rides r on rt.ride_id = r.ride_id
