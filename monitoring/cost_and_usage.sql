-- Warehouse cost & usage monitoring (BigQuery INFORMATION_SCHEMA). Replace `region-us` if needed.

-- 1) Storage by layer and table
select
    table_schema                                    as dataset,
    table_name,
    total_rows,
    round(total_logical_bytes / pow(1024, 3), 2)    as logical_gb,
    round(active_logical_bytes / pow(1024, 3), 2)   as active_gb,
    round(long_term_logical_bytes / pow(1024, 3), 2) as long_term_gb
from `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
where table_schema in ('raw', 'cleaned', 'business')
order by total_logical_bytes desc;

-- 2) Bytes billed per day and per user/service account (last 30 days)
select
    date(creation_time)                             as day,
    user_email,
    count(*)                                        as jobs,
    round(sum(total_bytes_billed) / pow(1024, 4), 3) as tib_billed,
    round(sum(total_bytes_billed) / pow(1024, 4) * 6.25, 2) as approx_on_demand_usd
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 30 day)
  and job_type = 'QUERY'
group by 1, 2
order by 1 desc, tib_billed desc;

-- 3) Most expensive queries hitting the fact table (candidates for a mart or better filters)
select
    creation_time,
    user_email,
    round(total_bytes_processed / pow(1024, 3), 2)  as gb_processed,
    total_slot_ms,
    substr(query, 1, 300)                           as query_preview
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 7 day)
  and job_type = 'QUERY'
  and exists (select 1 from unnest(referenced_tables) t where t.table_id = 'fct_trips')
order by total_bytes_processed desc
limit 20;

-- 4) dbt model runtimes (dbt labels every job with dbt_invocation_id via query comments)
select
    regexp_extract(query, r'"node_id": "([^"]+)"')  as dbt_node,
    count(*)                                        as runs,
    round(avg(timestamp_diff(end_time, start_time, second)), 1) as avg_seconds,
    round(avg(total_bytes_processed) / pow(1024, 3), 2) as avg_gb_processed
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 14 day)
  and query like '%"app": "dbt"%'
group by 1
order by avg_gb_processed desc;
