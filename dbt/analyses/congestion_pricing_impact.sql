-- Business question: what happened to yellow-taxi trips into the Manhattan congestion
-- relief zone after congestion pricing started on 2025-01-05?
-- Needs 2024-10 .. 2025-03 (or later) loaded. Compare the same weeks before and after.
with trips as (
    select
        pickup_date,
        pickup_date >= date '2025-01-05'                    as is_after,
        cbd_congestion_fee > 0                              as charged_cbd_fee,
        do_borough,
        total_amount,
        duration_min,
        avg_speed_mph
    from {{ ref('fct_trips') }}
    where pickup_date between date '2024-11-01' and date '2025-02-28'
      and not is_weekend
)
select
    is_after,
    count(*) / count(distinct pickup_date)                  as trips_per_weekday,
    countif(do_borough = 'Manhattan') / count(distinct pickup_date)
                                                            as manhattan_dropoffs_per_weekday,
    safe_divide(countif(charged_cbd_fee), count(*))         as share_charged_cbd_fee,
    avg(total_amount)                                       as avg_ticket,
    avg(avg_speed_mph)                                      as avg_speed_mph,
    avg(duration_min)                                       as avg_duration_min
from trips
group by is_after
order by is_after
