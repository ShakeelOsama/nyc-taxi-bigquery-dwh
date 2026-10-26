{#
  BigQuery Sandbox (no billing) expires every date partition older than 60 days, so
  partitioning historical data by trip/order date would delete it on write.
  With --vars '{sandbox: true}' models keep their clustering but skip date partitioning.
  On a billed project (the normal case) the partition spec is used as written.
#}
{% macro partition_if_allowed(partition_spec) -%}
    {{ return(none if var('sandbox', false) else partition_spec) }}
{%- endmacro %}
