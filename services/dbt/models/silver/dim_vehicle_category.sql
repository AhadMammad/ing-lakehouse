select
    category_id,
    code,
    display_name,
    base_fare,
    per_km_rate,
    per_min_rate,
    min_fare
from {{ ref('stg_rideon__vehicle_categories') }}
