-- Airport operations by month and airport: volume, direction, revenue, flat-fare usage, duration.
select
    date_trunc(pickup_date, month)                                  as month,
    airport_code,
    airport_direction,
    count(*)                                                        as trips,
    sum(total_amount)                                               as gross_revenue,
    avg(total_amount)                                               as avg_ticket,
    avg(duration_min)                                               as avg_duration_min,
    approx_quantiles(duration_min, 100)[offset(90)]                 as p90_duration_min,
    avg(trip_distance)                                              as avg_distance,
    safe_divide(countif(ratecode_id = 2), count(*))                 as jfk_flat_fare_share,
    safe_divide(countif(airport_fee > 0), count(*))                 as airport_fee_share,
    avg(tip_pct_of_fare)                                            as avg_card_tip_pct
from {{ ref('fct_trips') }}
where is_airport_trip
group by 1, 2, 3
