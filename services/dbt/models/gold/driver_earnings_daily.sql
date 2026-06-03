-- Per driver per day: earnings (from payouts) joined with ride activity and ratings.
with payouts as (
    select * from {{ ref('fact_driver_payouts') }}
),
drivers as (
    select * from {{ ref('dim_driver') }}
),
ratings as (
    select
        driver_id,
        rating_date,
        avg(cast(score as double)) as avg_rating_received,
        count(*)                   as ratings_received
    from {{ ref('fact_ratings') }}
    where rater_role = 'rider'   -- riders rate drivers
    group by driver_id, rating_date
)
select
    po.payout_date                                  as day,
    po.driver_id,
    d.full_name                                     as driver_name,
    d.city_id,
    d.city_name,
    po.rides_count,
    po.gross_amount,
    po.commission,
    po.net_amount,
    po.avg_fare_per_ride,
    po.currency,
    rt.avg_rating_received,
    rt.ratings_received
from payouts po
left join drivers d on po.driver_id = d.driver_id
left join ratings rt
       on po.driver_id = rt.driver_id
      and po.payout_date = rt.rating_date
