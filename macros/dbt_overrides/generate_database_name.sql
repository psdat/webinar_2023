{% macro generate_database_name(custom_database_name=none, node=none) -%}

     {%- if custom_database_name -%}
        {%- set database_name = custom_database_name -%}
    {%- else -%}
        {%- set database_name = target.database -%}
    {%- endif -%}

    {%- if target.name == 'dev' or target.name == 'uat' -%}
        {{ database_name ~ var('database_suffix') }}
    {%- else -%}
        {{ database_name }}
    {%- endif -%}

{%- endmacro %}