-- Tipping is only recorded for card payments, so this mart is restricted to card trips.
-- Shows where/when riders tip more and how often they tip nothing.
select
    date_trunc(pickup_date, month)                                  as month,
    pu_borough,
    time_band,
    distance_band,
    count(*)                                                        as card_trips,
    avg(tip_pct_of_fare)                                            as avg_tip_pct,
    approx_quantiles(tip_pct_of_fare, 100)[offset(50)]              as median_tip_pct,
    safe_divide(countif(tip_amount = 0), count(*))                  as zero_tip_rate,
    sum(tip_amount)                                                 as total_tips
from {{ ref('fct_trips') }}
where tip_is_recorded
  and fare_amount > 0
group by 1, 2, 3, 4
