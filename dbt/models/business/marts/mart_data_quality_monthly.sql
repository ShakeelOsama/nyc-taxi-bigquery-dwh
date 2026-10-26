-- Data-quality scorecard per source month: raw rows in, valid rows out, rejects by rule.
-- Powers the DQ page of the dashboard and the volume numbers quoted in the README.
with raw_counts as (
    select source_month, count(*) as raw_rows
    from {{ source('tlc_raw', 'yellow_trips') }}
    group by source_month
),

valid as (
    select source_month, count(*) as rows, countif(had_source_duplicates) as dup_flagged
    from {{ ref('stg_yellow_trips') }}
    group by source_month
),

rejected as (
    select source_month, reject_reason, count(*) as rows
    from {{ ref('stg_yellow_trips__rejected') }}
    group by source_month, reject_reason
),

outcomes as (
    select source_month, 'accepted' as outcome, rows from valid
    union all
    select source_month, concat('rejected:', reject_reason), rows from rejected
)

select
    o.source_month,
    o.outcome,
    o.rows,
    r.raw_rows,
    safe_divide(o.rows, r.raw_rows)                                 as share_of_raw,
    -- raw minus (accepted + rejected) = exact duplicates removed
    r.raw_rows - sum(o.rows) over (partition by o.source_month)     as exact_duplicates_removed
from outcomes as o
join raw_counts as r using (source_month)
