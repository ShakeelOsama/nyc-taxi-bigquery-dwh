{#
  Returns {'lo': 'YYYY-MM-DD', 'hi': 'YYYY-MM-DD'} = the range of source months that were
  (re)loaded upstream since this model last ran, or none on a full refresh.

  Why compile-time: BigQuery only prunes partitions when the filter is a constant. A filter
  like `where source_month in (select ... )` would scan every partition of a multi-GB table
  on every run. Resolving the window with run_query() first turns it into literal dates,
  so an incremental run touches only the changed months.
#}
{% macro changed_month_window(upstream, month_column, upstream_watermark, this_watermark) %}
    {% if not execute or not is_incremental() %}
        {{ return(none) }}
    {% endif %}
    {% set q %}
        select
            format_date('%Y-%m-%d', min({{ month_column }})) as lo,
            format_date('%Y-%m-%d', max({{ month_column }})) as hi
        from {{ upstream }}
        where {{ upstream_watermark }} > (
            select coalesce(max({{ this_watermark }}), timestamp '1970-01-01') from {{ this }}
        )
    {% endset %}
    {% set row = run_query(q).rows[0] %}
    {% do log("Incremental window for " ~ this ~ ": " ~ row[0] ~ " .. " ~ row[1], info=true) %}
    {{ return({'lo': row[0], 'hi': row[1]}) }}
{% endmacro %}


{# Renders the WHERE clause for an incremental run (or nothing on full refresh). #}
{% macro window_filter(window, column, month_end=false) %}
    {%- if is_incremental() -%}
        {%- if window and window.lo -%}
            where {{ column }} between date '{{ window.lo }}' and
            {% if month_end %}last_day(date '{{ window.hi }}', month){% else %}date '{{ window.hi }}'{% endif %}
        {%- else -%}
            where false   -- nothing new upstream
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
