{#
  prod  -> datasets are exactly `cleaned` and `business` (what BI tools point at)
  other -> `<target dataset>_cleaned`, e.g. dev_osama_cleaned, ci_1234_business,
           so developers and CI never overwrite production tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
