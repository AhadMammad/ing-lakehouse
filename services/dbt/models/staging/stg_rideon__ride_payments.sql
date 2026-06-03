with src as (
    select * from {{ source('bronze', 'rideon_ride_payments_raw') }}
),
deduped as (
    select *,
        row_number() over (partition by payment_id order by ingest_ts desc) as _rn
    from src
)
select
    payment_id,
    ride_id,
    rider_id,
    method,
    amount,
    currency,
    status,
    created_at,
    updated_at
from deduped
where _rn = 1
