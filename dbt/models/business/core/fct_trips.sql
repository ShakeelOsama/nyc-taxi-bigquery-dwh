{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['pu_location_id', 'do_location_id', 'payment_type_code'],
    on_schema_change = 'append_new_columns'
  )
}}

-- Grain: one row per valid yellow-taxi trip (tens of millions of rows per year).
-- Rebuilds only the pickup_date partitions of months that changed in the cleaned layer.
{% set window = changed_month_window(
    ref('stg_yellow_trips'), 'source_month', '_dbt_loaded_at', '_stg_loaded_at') %}

with trips as (

    select * from {{ ref('stg_yellow_trips') }}
    {{ window_filter(window, 'pickup_date', month_end=true) }}

),

pu as (select * from {{ ref('dim_zones') }}),
dz as (select * from {{ ref('dim_zones') }})

select
    t.trip_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.source_month,
    extract(hour from t.pickup_datetime)                                as pickup_hour,
    extract(dayofweek from t.pickup_datetime)                           as pickup_day_of_week,
    extract(dayofweek from t.pickup_datetime) in (1, 7)                 as is_weekend,
    case
        when extract(hour from t.pickup_datetime) between 7 and 9   then 'am_peak'
        when extract(hour from t.pickup_datetime) between 10 and 15 then 'midday'
        when extract(hour from t.pickup_datetime) between 16 and 19 then 'pm_peak'
        when extract(hour from t.pickup_datetime) between 20 and 23 then 'evening'
        else 'overnight'
    end                                                                 as time_band,

    t.vendor_id,
    v.vendor_name,

    t.pu_location_id,
    pu.borough                                                          as pu_borough,
    pu.zone                                                             as pu_zone,
    t.do_location_id,
    dz.borough                                                          as do_borough,
    dz.zone                                                             as do_zone,
    pu.is_airport or dz.is_airport                                      as is_airport_trip,
    coalesce(pu.airport_code, dz.airport_code)                          as airport_code,
    case when pu.is_airport then 'from_airport' when dz.is_airport then 'to_airport' end
                                                                        as airport_direction,
    t.pu_location_id = t.do_location_id                                 as is_same_zone_trip,

    t.ratecode_id,
    r.rate_code,
    t.payment_type_code,
    p.payment_type,
    coalesce(p.records_tip, false)                                      as tip_is_recorded,

    t.passenger_count,
    t.trip_distance,
    t.duration_min,
    safe_divide(t.trip_distance, t.duration_min / 60)                   as avg_speed_mph,
    case
        when t.trip_distance < 1  then '0-1 mi'
        when t.trip_distance < 3  then '1-3 mi'
        when t.trip_distance < 7  then '3-7 mi'
        when t.trip_distance < 15 then '7-15 mi'
        else '15+ mi'
    end                                                                 as distance_band,

    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.extra + t.mta_tax + t.improvement_surcharge
      + t.congestion_surcharge + t.airport_fee + t.cbd_congestion_fee   as surcharges_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount,
    safe_divide(t.fare_amount, t.trip_distance)                         as fare_per_mile,
    if(coalesce(p.records_tip, false), safe_divide(t.tip_amount, t.fare_amount), null)
                                                                        as tip_pct_of_fare,

    t.is_passenger_count_missing,
    t.is_unknown_zone,
    t._dbt_loaded_at                                                    as _stg_loaded_at,
    current_timestamp()                                                 as _dbt_loaded_at

from trips as t
left join pu on t.pu_location_id = pu.location_id
left join dz on t.do_location_id = dz.location_id
left join {{ ref('payment_types') }} as p on t.payment_type_code = p.payment_type_code
left join {{ ref('rate_codes') }}    as r on t.ratecode_id = r.ratecode_id
left join {{ ref('vendors') }}       as v on t.vendor_id = v.vendor_id
