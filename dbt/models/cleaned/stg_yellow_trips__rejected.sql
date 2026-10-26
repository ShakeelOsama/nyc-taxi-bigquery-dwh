{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'source_month', 'data_type': 'date', 'granularity': 'month'},
    cluster_by = ['reject_reason'],
    on_schema_change = 'append_new_columns'
  )
}}

-- Quarantine: every raw trip that failed a validation rule, with the reason.
-- Nothing is silently dropped; analysts can audit and data owners can fix upstream.
{% set window = changed_month_window(
    source('tlc_raw', 'yellow_trips'), 'source_month', '_ingested_at', '_raw_ingested_at') %}

{{ validated_trips(window) }}

select
    trip_id,
    source_month,
    reject_reason,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    duration_min,
    trip_distance,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    total_amount,
    _source_file,
    _ingested_at                                        as _raw_ingested_at,
    current_timestamp()                                 as _dbt_loaded_at
from validated
where reject_reason is not null
