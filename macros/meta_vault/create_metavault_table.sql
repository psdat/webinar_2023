{# Macro that creates the metadata vault table #}

{% macro create_metavault_table() %}

{%- set meta_vault_delta_database_name -%}
    {{ 'META_VAULT' ~ var('database_suffix') }}
{%- endset -%}

{%- set meta_vault_delta_schema_name -%}
    {{ var('schema_prefix') ~ 'DBT' }}
{%- endset -%}

{%- set meta_vault_delta_table_name -%}
    {{ var('meta_vault_delta_table_name') }}
{%- endset -%}

{{ create_metavault_schema( target.database , meta_vault_delta_schema_name ) }}

{%- if flags.FULL_REFRESH and var('full_refresh_force') -%}
    CREATE OR REPLACE TABLE
{%- else -%}
    CREATE TABLE IF NOT EXISTS  
{%- endif %}
{{ target.database }}.{{ meta_vault_delta_schema_name }}.{{ meta_vault_delta_table_name }} (
    INVOCATION_ID VARCHAR,
    VIEW_NAME VARCHAR,
    LOAD_DATE_TIMESTAMP VARCHAR,
    ROW_COUNT NUMBER(18,0),
    MIN_FIVETRAN_SYNCED VARCHAR,
    MAX_FIVETRAN_SYNCED VARCHAR,
    QUERY_TAG VARCHAR
);

{% endmacro %}