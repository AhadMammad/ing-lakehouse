-- Per rider: lifetime ride/spend metrics (full recompute each run).
with rides as (
    select * from {{ ref('fact_rides') }}
),
payments as (
    select rider_id, amount, status
    from {{ ref('fact_ride_payments') }}
),
riders as (
    select * from {{ ref('dim_rider') }}
),
ride_agg as (
    select
        rider_id,
        count(*)                                          as total_rides,
        count_if(is_completed)                            as completed_rides,
        count_if(is_cancelled)                            as cancelled_rides,
        min(ride_date)                                    as first_ride_date,
        max(ride_date)                                    as last_ride_date,
        round(avg(case when is_completed then distance_km end), 2) as avg_distance_km
    from rides
    group by rider_id
),
pay_agg as (
    select
        rider_id,
        coalesce(sum(case when status = 'captured' then amount end), 0) as total_spend,
        round(avg(case when status = 'captured' then amount end), 2)    as avg_fare
    from payments
    group by rider_id
)
select
    r.rider_id,
    rd.full_name           as rider_name,
    rd.city_name,
    rd.country,
    r.total_rides,
    r.completed_rides,
    r.cancelled_rides,
    round(100.0 * r.cancelled_rides / nullif(r.total_rides, 0), 2) as cancellation_rate_pct,
    p.total_spend,
    p.avg_fare,
    r.avg_distance_km,
    r.first_ride_date,
    r.last_ride_date
from ride_agg r
left join pay_agg p on r.rider_id = p.rider_id
left join riders rd on r.rider_id = rd.rider_id
