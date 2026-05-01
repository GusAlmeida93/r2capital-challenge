{# Use the custom schema literally, without prefixing the target schema.
   Default dbt behavior turns +schema: gold into <target_schema>_gold, which is
   awkward for analytics consumers. Override emits raw/silver/gold as-is and
   keeps snapshots in the same `raw` schema declared on their config. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
