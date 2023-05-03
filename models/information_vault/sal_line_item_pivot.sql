{%- set column_values_list -%}
    {{ "'" ~ dbt_utils.get_column_values(ref('sal_line_item'), 'BUSINESS_KEY_COLLISION_CODE', order_by='BUSINESS_KEY_COLLISION_CODE asc') | join("', '") ~ "'" }}
{%- endset -%}

{%- set column_headers_list -%}
    {{ dbt_utils.get_column_values(ref('sal_line_item'), 'BUSINESS_KEY_COLLISION_CODE', order_by='BUSINESS_KEY_COLLISION_CODE asc') | join("_LINE_ITEM_HASH_KEY,") ~ '_LINE_ITEM_HASH_KEY' }}
{%- endset -%}

with sal as (
    select
        SAL_LINE_ITEM_HASH_KEY,
        LINE_ITEM_HASH_KEY,
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('sal_line_item') }}
),
pivot_sal as (
    select 
        *
    from sal
    --PIVOT(MIN(LINE_ITEM_HASH_KEY) FOR BUSINESS_KEY_COLLISION_CODE IN ('RESOURCE_MANAGER','HUBSPOT'))
    --AS p (SAL_LINE_ITEM_HASH_KEY,RESOURCE_MANAGER_LINE_ITEM_HASH_KEY,HUBSPOT_LINE_ITEM_HASH_KEY )
    PIVOT(MAX(LINE_ITEM_HASH_KEY) FOR BUSINESS_KEY_COLLISION_CODE IN ({{ column_values_list }}))
    AS p (SAL_LINE_ITEM_HASH_KEY,{{ column_headers_list }} )
)

select * from pivot_sal