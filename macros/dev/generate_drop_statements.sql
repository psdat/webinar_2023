{# This helper macro scans for all schemas in the data vault databases, then generates SQL statements to drop them #}
{# The macro will not execute the statements. This macro should only be invocated by models in the analysis path using `dbt compile` #}

{% macro generate_drop_statements() %}
    
    {% set query %}
        
        SELECT
            DATABASE_NAME
        FROM
            SNOWFLAKE.INFORMATION_SCHEMA.DATABASES
        WHERE
            DATABASE_NAME ILIKE '{{ '%_VAULT%' ~ var('database_suffix') }}'
        
    {% endset %}

    {# {{ log("query: " ~ query, True) }} #}

    {% set results = run_query(query) %}

    {# {{ log("results: " ~ results, True) }} #}

    {% if execute %}
        {% set results_list = results.rows %}
        {% else %}
        {% set results_list = [] %}
    {% endif %}

    {% set drop_all_schemas = [] %}

    {% for db in results_list %}
        {# {{ log("res value: " ~ db[0], true) }} #}

        {% set schema_list = get_schemas(db[0]) %}

        {% for schema in schema_list %}
            
            {{ drop_all_schemas.append('DROP SCHEMA ' + db[0] + '.' + schema[0] + ';') }}

        {% endfor %}

    {% endfor %}

    {% set drop_all_schemas_sql = drop_all_schemas|join("\n") %}

    {# {{ log(drop_all_schemas_sql, true) }} #}

    {{ return(drop_all_schemas_sql) }}

{% endmacro %}

{% macro get_schemas(database) %}
    
    {% set query %}
        
        SELECT
            SCHEMA_NAME
        FROM
            {{ database }}.INFORMATION_SCHEMA.SCHEMATA
        WHERE
            SCHEMA_NAME != 'INFORMATION_SCHEMA'
        {%- if target.name == 'dev' -%}
            AND
                SCHEMA_NAME ILIKE '{{ target.schema }}_%'
        {%- endif -%}

    {% endset %}

    {# {{ log("query: " ~ query, True) }} #}

    {% set results = run_query(query) %}

    {# {{ log("results: " ~ results, True) }} #}

    {% if execute %}
        {% set results_list = results.rows %}
        {% else %}
        {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}