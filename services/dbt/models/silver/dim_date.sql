-- Date dimension generated from the observed span of ride activity.
-- Uses Trino's sequence()/unnest() so no external packages are needed.
with bounds as (
    select
        cast(min(requested_at) as date) as min_d,
        cast(max(requested_at) as date) as max_d
    from {{ ref('stg_rideon__rides') }}
),
spine as (
    select d as date_day
    from bounds
    cross join unnest(sequence(bounds.min_d, bounds.max_d, interval '1' day)) as t(d)
)
select
    date_day,
    year(date_day)                                       as year,
    quarter(date_day)                                    as quarter,
    month(date_day)                                      as month,
    day(date_day)                                        as day,
    day_of_week(date_day)                                as day_of_week,   -- 1=Mon..7=Sun
    date_format(date_day, '%W')                          as day_name,
    week(date_day)                                       as iso_week,
    day_of_week(date_day) in (6, 7)                      as is_weekend
from spine
