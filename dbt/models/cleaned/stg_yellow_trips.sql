{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['pu_location_id', 'do_location_id'],
    on_schema_change = 'append_new_columns'
  )
}}

{% set window = changed_month_window(
    source('tlc_raw', 'yellow_trips'), 'source_month', '_ingested_at', '_raw_ingested_at') %}

{{ validated_trips(window) }}

select
    trip_id,
    vendor_id,
    pickup_datetime,                                    -- NYC local time, as published
    dropoff_datetime,
    date(pickup_datetime)                               as pickup_date,
    source_month,
    passenger_count,
    trip_distance,
    duration_min,
    ratecode_id,
    upper(store_and_fwd_flag) = 'Y'                     as is_store_and_forward,
    pu_location_id,
    do_location_id,
    payment_type                                        as payment_type_code,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    coalesce(congestion_surcharge, 0)                   as congestion_surcharge,
    coalesce(airport_fee, 0)                            as airport_fee,
    coalesce(cbd_congestion_fee, 0)                     as cbd_congestion_fee,
    total_amount,

    -- soft data-quality flags: kept, but visible to analysts
    coalesce(passenger_count, 0) = 0                    as is_passenger_count_missing,
    pu_location_id in (264, 265) or do_location_id in (264, 265)
                                                        as is_unknown_zone,
    duplicate_count > 1                                 as had_source_duplicates,

    _ingested_at                                        as _raw_ingested_at,
    current_timestamp()                                 as _dbt_loaded_at
from validated
where reject_reason is null
