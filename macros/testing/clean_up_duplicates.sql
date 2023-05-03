{% macro clean_up_duplicates() %}

{% set query %}
    SELECT 1 FROM DUAL
        {# WITH DUPLICATE_ROWS AS (
        SELECT
            *,
            ROW_NUMBER() OVER(
                    PARTITION BY
                        CUSTOMER_HASH_KEY,
                        CHILD_DEPENDENCY_HASH_KEY,
                        HASHDIFF
                    ORDER BY
                        CUSTOMER_HASH_KEY,
                        CHILD_DEPENDENCY_HASH_KEY,
                        HASHDIFF) AS ROWNUMBER
        FROM 
            {{ ref('masat_customer_bonhams_address_pii') }} 
        )
        DELETE FROM DUPLICATE_ROWS WHERE ROWNUMBER>1 #}
{% endset %}

{% do run_query(query) %}

{% endmacro %}