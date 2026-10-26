-- Run both in the BigQuery console and compare "bytes processed" in the job information.
-- This is the cost story for the screen-share.

-- (A) pruned: constant filter on the partition column + clustered column
select pu_zone, count(*) as trips, sum(total_amount) as revenue
from {{ ref('fct_trips') }}
where pickup_date between date '2024-03-01' and date '2024-03-31'
  and pu_location_id = 132          -- JFK Airport
group by pu_zone;

-- (B) not pruned: wrapping the partition column in a function hides it from the planner
-- select pu_zone, count(*) as trips, sum(total_amount) as revenue
-- from {{ ref('fct_trips') }}
-- where format_date('%Y-%m', pickup_date) = '2024-03'
--   and pu_location_id = 132
-- group by pu_zone;
