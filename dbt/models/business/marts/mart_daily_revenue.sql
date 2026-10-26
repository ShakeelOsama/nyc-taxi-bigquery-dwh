{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = partition_if_allowed({'field': 'pickup_date', 'data_type': 'date', 'granularity': 'month'})
  )
}}

-- Daily revenue and operations KPIs (finance + ops dashboard). Incremental like the fact,
-- so a monthly load re-aggregates ~30 days instead of the full history.
{% set window = changed_month_window(
    ref('fct_trips'), 'source_month', '_dbt_loaded_at', '_fct_loaded_at') %}

select
    t.pickup_date,
    count(*)                                                    as trips,
    sum(t.passenger_count)                                      as passengers,
    sum(t.trip_distance)                                        as total_miles,
    sum(t.total_amount)                                         as gross_revenue,
    sum(t.fare_amount)                                          as fare_revenue,
    sum(t.tip_amount)                                           as tips,
    sum(t.tolls_amount)                                         as tolls,
    sum(t.surcharges_amount)                                    as surcharges,
    sum(t.congestion_surcharge)                                 as congestion_surcharge,
    sum(t.cbd_congestion_fee)                                   as cbd_congestion_fee,
    avg(t.total_amount)                                         as avg_ticket,
    avg(t.duration_min)                                         as avg_duration_min,
    approx_quantiles(t.duration_min, 100)[offset(50)]           as median_duration_min,
    safe_divide(sum(t.fare_amount), sum(t.trip_distance))       as fare_per_mile,
    safe_divide(countif(t.payment_type_code = 1), count(*))     as card_share,
    safe_divide(countif(t.is_airport_trip), count(*))           as airport_trip_share,
    max(t._dbt_loaded_at)                                       as _fct_loaded_at
from {{ ref('fct_trips') }} as t
{{ window_filter(window, 'pickup_date', month_end=true) }}
group by t.pickup_date
