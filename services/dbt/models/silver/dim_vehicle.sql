with vehicles as (
    select * from {{ ref('stg_rideon__vehicles') }}
),
categories as (
    select * from {{ ref('stg_rideon__vehicle_categories') }}
)
select
    v.vehicle_id,
    v.driver_id,
    v.category_id,
    cat.code         as category_code,
    cat.display_name as category_name,
    v.make,
    v.model,
    v.make || ' ' || v.model as make_model,
    v.year,
    v.plate_number,
    v.color,
    v.registered_at
from vehicles v
left join categories cat on v.category_id = cat.category_id
