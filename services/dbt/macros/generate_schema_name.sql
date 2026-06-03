{#
  Map dbt's schema name 1:1 to a Nessie iceberg namespace.

  dbt's default builds `<target_schema>_<custom_schema>` (e.g. `silver_gold`),
  which would create the wrong namespace. We want `+schema: gold` to resolve
  to the namespace `gold` exactly, and a model with no custom schema to use
  the target schema (`silver`).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
