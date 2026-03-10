{#
    Custom schema name generation macro
    
    By default, dbt concatenates the target schema with custom schema:
    {target_schema}_{custom_schema}
    
    This macro overrides that behavior to use ONLY the custom schema name
    when one is provided, giving you full control over dataset names.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# No custom schema specified, use the target schema #}
        {{ default_schema }}
    
    {%- else -%}
        {# Custom schema specified, use it directly without concatenation #}
        {{ custom_schema_name | trim }}
    
    {%- endif -%}

{%- endmacro %}
