{#
  Shared validation logic for the cleaned layer. Produces every raw trip with:
    * trip_id        deterministic surrogate key (TLC publishes no trip id)
    * duplicate_count number of exact copies found in the source (kept once)
    * reject_reason  NULL for a valid trip, otherwise the FIRST rule it breaks
  stg_yellow_trips keeps reject_reason is null; stg_yellow_trips__rejected keeps the rest,
  so no row is silently dropped and data quality is measurable per month and rule.
  Emits a WITH clause ending in a CTE named `validated`; the calling model selects from it.
#}
{% macro validated_trips(window) %}

with source as (

    select * from (
        select * from {{ source('tlc_raw', 'yellow_trips') }}
        {{ window_filter(window, 'source_month') }}
    )
    {% if var('ci_month', none) %}
    where source_month = date '{{ var("ci_month") }}'   -- CI builds a single month
    {% endif %}

),

keyed as (

    select
        *,
        farm_fingerprint(to_json_string(struct(
            vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
            ratecode_id, pu_location_id, do_location_id, payment_type,
            fare_amount, tip_amount, tolls_amount, total_amount
        )))                                                             as trip_id,
        datetime_diff(dropoff_datetime, pickup_datetime, second) / 60.0 as duration_min
    from source

),

deduped as (

    select
        *,
        count(*) over (partition by trip_id)                            as duplicate_count
    from keyed
    where true
    qualify row_number() over (partition by trip_id order by _ingested_at desc) = 1

)

, validated as (

select
    *,
    case
        when pickup_datetime is null or dropoff_datetime is null
            then 'missing_timestamp'
        when date_trunc(date(pickup_datetime), month) != source_month
            then 'pickup_outside_file_month'
        when dropoff_datetime <= pickup_datetime
            then 'non_positive_duration'
        when duration_min > {{ var('max_trip_hours') }} * 60
            then 'duration_too_long'
        when trip_distance is null or trip_distance <= 0
            then 'zero_distance'
        when trip_distance > {{ var('max_trip_miles') }}
            then 'distance_outlier'
        when safe_divide(trip_distance, duration_min / 60) > {{ var('max_speed_mph') }}
            then 'impossible_speed'
        when fare_amount < 0 or total_amount < 0
            then 'negative_amount_void_or_refund'
        when total_amount > {{ var('max_total_amount') }}
            then 'amount_outlier'
        when pu_location_id is null or do_location_id is null
            then 'missing_location'
    end                                                                 as reject_reason
from deduped

)

{% endmacro %}
