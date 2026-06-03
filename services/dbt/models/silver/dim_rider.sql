with riders as (
    select * from {{ ref('stg_rideon__riders') }}
),
cities as (
    select * from {{ ref('stg_rideon__cities') }}
)
select
    r.rider_id,
    r.first_name,
    r.last_name,
    r.first_name || ' ' || r.last_name as full_name,
    r.email,
    r.phone,
    r.city_id,
    c.city_name,
    c.country,
    r.rating,
    r.created_at,
    cast(r.created_at as date) as signup_date
from riders r
left join cities c on r.city_id = c.city_id
