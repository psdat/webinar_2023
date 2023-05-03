{# Iterates through each v_stg model and unions the invocation_id, model_name, row_count and min/max LDTSs for inserting into the meta_vault #}

{%- macro update_dbt_run_deltas() -%}

    {%- set database_name = target.database ~ var('database_suffix') -%}
    {%- set schema_pattern = '%staging%' -%}
    {%- set table_pattern = 'v\_stg\_%' -%}

    {%- set meta_vault_delta_schema_name -%}
        {{ var('schema_prefix') ~ 'DBT' }}
    {%- endset -%}

    {%- set meta_vault_delta_table_name -%}
        {{ var('meta_vault_delta_table_name') }}
    {%- endset -%}


    {%- set tables = dbt_utils.get_relations_by_pattern(database=database_name, schema_pattern=schema_pattern, table_pattern=table_pattern) -%}

    {%- set update_delta_query -%}
        insert into {{ target.database }}.{{ meta_vault_delta_schema_name }}.{{ meta_vault_delta_table_name }}
        with update_query as (
        {% for table in tables %}

            {%- if not loop.first -%}
                UNION ALL
            {%- endif %}

            SELECT
                '{{ invocation_id }}' AS INVOCATION_ID,
                '{{ table.database | lower }}.{{ table.schema | lower  }}.{{ table.name | lower }}' AS VIEW_NAME,
                '{{ run_started_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] }}' AS LOAD_DATE_TIMESTAMP,
                COUNT(*) AS ROW_COUNT,
                MIN(LOAD_DATE_TIMESTAMP) AS MIN_FIVETRAN_SYNCED,
                MAX(LOAD_DATE_TIMESTAMP) AS MAX_FIVETRAN_SYNCED,
                '{{ 'invocation_id: ' ~ invocation_id ~ ' | ' ~ 'model.name: ' ~ table.name | lower }}' AS QUERY_TAG
            FROM
                {{ table.database }}.{{ table.schema }}.{{ table.name }}

        {% endfor %}

        )
        select * from update_query

    {%- endset -%}


    {{ return(update_delta_query) }}

{%- endmacro -%}