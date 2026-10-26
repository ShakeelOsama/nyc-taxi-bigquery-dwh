{{ config(materialized='table') }}

-- US federal holidays flagged for the most common demand-impacting dates.
with bounds as (
    select
        date_trunc(min(pickup_date), year)  as start_date,
        last_day(max(pickup_date), year)    as end_date
    from {{ ref('stg_yellow_trips') }}
)

select
    d                                                   as date_day,
    extract(year from d)                                as year,
    extract(quarter from d)                             as quarter,
    extract(month from d)                               as month,
    date_trunc(d, month)                                as month_start,
    date_trunc(d, isoweek)                              as week_start,
    format_date('%A', d)                                as day_name,
    extract(dayofweek from d)                           as day_of_week,
    extract(dayofweek from d) in (1, 7)                 as is_weekend,
    (extract(month from d) = 1  and extract(day from d) = 1)
      or (extract(month from d) = 7  and extract(day from d) = 4)
      or (extract(month from d) = 12 and extract(day from d) = 25)
      -- Thanksgiving: 4th Thursday of November
      or (extract(month from d) = 11 and extract(dayofweek from d) = 5 and extract(day from d) between 22 and 28)
                                                        as is_major_holiday
from bounds, unnest(generate_date_array(start_date, end_date)) as d
