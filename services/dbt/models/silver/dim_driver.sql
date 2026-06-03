with drivers as (
    select * from {{ ref('stg_rideon__drivers') }}
),
cities as (
    select * from {{ ref('stg_rideon__cities') }}
)
select
    d.driver_id,
    d.first_name,
    d.last_name,
    d.first_name || ' ' || d.last_name as full_name,
    d.email,
    d.phone,
    d.city_id,
    c.city_name,
    c.country,
    d.license_number,
    d.status,
    d.rating,
    d.onboarded_at,
    cast(d.onboarded_at as date) as onboarded_date
from drivers d
left join cities c on d.city_id = c.city_id
