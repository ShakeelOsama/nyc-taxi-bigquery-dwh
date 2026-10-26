-- Volume anomaly guard: if more than 10% of a month's raw rows are rejected, something
-- upstream changed (new file format, unit change, broken load) and business tables
-- should not be refreshed until someone looks.

select source_month, sum(share_of_raw) as reject_share
from {{ ref('mart_data_quality_monthly') }}
where outcome like 'rejected:%'
group by source_month
having sum(share_of_raw) > 0.10
