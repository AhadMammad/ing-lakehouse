with src as (
    select * from {{ source('bronze', 'rideon_driver_payouts_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by payout_id order by ingest_ts desc) as _rn
    from src
)
select
    payout_id,
    driver_id,
    payout_date,
    gross_amount,
    commission,
    net_amount,
    currency,
    rides_count,
    status
from deduped
where _rn = 1
