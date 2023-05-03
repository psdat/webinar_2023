{% macro create_metavault_schema(db,schema) %}

CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ schema }};

{% endmacro %}