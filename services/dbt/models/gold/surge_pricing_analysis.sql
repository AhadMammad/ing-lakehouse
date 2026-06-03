-- Per city / category / day: surge prevalence and revenue uplift.
with rides as (
    select * from {{ ref('fact_rides') }}
),
cities as (
    select city_id, city_name, country from {{ ref('dim_city') }}
),
categories as (
    select category_id, code, display_name from {{ ref('dim_vehicle_category') }}
)
select
    r.ride_date                                              as day,
    r.city_id,
    c.city_name,
    r.category_id,
    cat.code                                                as category_code,
    count(*)                                                as total_rides,
    count_if(r.is_surge)                                    as surge_rides,
    round(100.0 * count_if(r.is_surge) / count(*), 2)       as surge_ride_pct,
    round(avg(r.surge_multiplier), 3)                       as avg_surge_multiplier,
    round(max(r.surge_multiplier), 2)                       as max_surge_multiplier,
    coalesce(sum(r.surge_amount), 0)                        as surge_revenue,
    coalesce(sum(r.total_fare), 0)                          as gross_fare
from rides r
left join cities c on r.city_id = c.city_id
left join categories cat on r.category_id = cat.category_id
group by r.ride_date, r.city_id, c.city_name, r.category_id, cat.code
