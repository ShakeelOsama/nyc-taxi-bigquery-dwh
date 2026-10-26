-- Layer reconciliation: every source month must have the same trip count and revenue in
-- cleaned and business. Catches broken incremental windows and fan-out from bad joins.
with c as (
    select source_month, count(*) as trips, round(sum(total_amount), 2) as revenue
    from {{ ref('stg_yellow_trips') }}
    group by source_month
),
b as (
    select source_month, count(*) as trips, round(sum(total_amount), 2) as revenue
    from {{ ref('fct_trips') }}
    group by source_month
)
select
    coalesce(c.source_month, b.source_month) as source_month,
    c.trips as cleaned_trips, b.trips as business_trips,
    c.revenue as cleaned_revenue, b.revenue as business_revenue
from c
full outer join b using (source_month)
where c.trips is distinct from b.trips
   or abs(coalesce(c.revenue, 0) - coalesce(b.revenue, 0)) > 1
