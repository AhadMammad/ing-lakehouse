-- Per city per day: demand, fulfilment, and revenue KPIs.
with rides as (
    select * from {{ ref('fact_rides') }}
),
cities as (
    select * from {{ ref('dim_city') }}
)
select
    r.ride_date                                              as day,
    r.city_id,
    c.city_name,
    c.country,
    count(*)                                                 as total_rides,
    count_if(r.is_completed)                                 as completed_rides,
    count_if(r.is_cancelled)                                 as cancelled_rides,
    round(100.0 * count_if(r.is_cancelled) / count(*), 2)    as cancellation_rate_pct,
    coalesce(sum(r.total_fare), 0)                           as gross_fare,
    round(avg(r.surge_multiplier), 3)                        as avg_surge,
    round(avg(case when r.is_completed then r.distance_km end), 2) as avg_distance_km,
    round(avg(case when r.is_completed then r.duration_min end), 2) as avg_duration_min,
    count(distinct r.rider_id)                               as unique_riders,
    count(distinct r.driver_id)                              as unique_drivers
from rides r
left join cities c on r.city_id = c.city_id
group by r.ride_date, r.city_id, c.city_name, c.country
