select
    city_id,
    city_name,
    country,
    timezone,
    launched_at,
    cast(launched_at as date) as launched_date
from {{ ref('stg_rideon__cities') }}
