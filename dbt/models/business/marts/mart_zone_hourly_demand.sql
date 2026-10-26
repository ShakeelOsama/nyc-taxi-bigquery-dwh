{{ config(cluster_by=['pu_borough', 'pu_zone']) }}

-- Demand heatmap for fleet positioning: for each pickup zone x weekday x hour,
-- how many trips start in an average hour and how much an hour of driving there earns.
-- Uses the last 12 loaded months so seasonal changes roll off automatically.
with params as (
    select date_sub(max(pickup_date), interval 12 month) as from_date
    from {{ ref('mart_daily_revenue') }}
),

trips as (
    select t.*
    from {{ ref('fct_trips') }} as t
    cross join params
    where t.pickup_date > params.from_date
      and not t.is_unknown_zone
),

day_counts as (
    -- number of calendar days per weekday in the window, to turn totals into hourly averages
    select pickup_day_of_week, count(distinct pickup_date) as n_days
    from trips
    group by pickup_day_of_week
)

select
    t.pu_location_id,
    t.pu_borough,
    t.pu_zone,
    t.pickup_day_of_week,
    t.pickup_hour,
    count(*)                                                        as trips,
    count(*) / any_value(d.n_days)                                  as avg_trips_per_hour,
    avg(t.total_amount)                                             as avg_ticket,
    avg(t.duration_min)                                             as avg_duration_min,
    avg(t.avg_speed_mph)                                            as avg_speed_mph,
    -- revenue per occupied hour: what a driver earns per hour spent with a passenger in this slot
    safe_divide(sum(t.total_amount), sum(t.duration_min) / 60)      as revenue_per_occupied_hour,
    safe_divide(countif(t.is_airport_trip), count(*))               as airport_share
from trips as t
join day_counts as d using (pickup_day_of_week)
group by 1, 2, 3, 4, 5
