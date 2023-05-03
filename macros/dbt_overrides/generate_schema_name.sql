{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name -%}
        {%- set schema_name = custom_schema_name -%}
    {%- else -%}
        {%- set schema_name = target.schema -%}
    {%- endif -%}

    {%- if target.name == 'dev' -%}
        {{ var('schema_prefix') ~ schema_name }}
    {%- else -%}
        {{ schema_name }}
    {%- endif -%}

{%- endmacro %}