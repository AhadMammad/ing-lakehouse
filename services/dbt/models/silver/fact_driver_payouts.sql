-- Grain: one row per driver per payout day.
with payouts as (
    select * from {{ ref('stg_rideon__driver_payouts') }}
),
drivers as (
    select driver_id, city_id from {{ ref('stg_rideon__drivers') }}
)
select
    po.payout_id,
    po.payout_date,
    po.driver_id,
    d.city_id,
    po.gross_amount,
    po.commission,
    po.net_amount,
    po.currency,
    po.rides_count,
    po.status,
    case when po.rides_count > 0
         then po.gross_amount / po.rides_count
         else 0 end as avg_fare_per_ride
from payouts po
left join drivers d on po.driver_id = d.driver_id
