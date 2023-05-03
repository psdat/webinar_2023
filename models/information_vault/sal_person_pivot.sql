{%- set column_values_list -%}
    {{ "'" ~ dbt_utils.get_column_values(ref('sal_person'), 'BUSINESS_KEY_COLLISION_CODE', order_by='BUSINESS_KEY_COLLISION_CODE asc') | join("', '") ~ "'" }}
{%- endset -%}

{%- set column_headers_list -%}
    {{ dbt_utils.get_column_values(ref('sal_person'), 'BUSINESS_KEY_COLLISION_CODE', order_by='BUSINESS_KEY_COLLISION_CODE asc') | join("_PERSON_HASH_KEY,") ~ '_PERSON_HASH_KEY' }}
{%- endset -%}


with sal as (
    select
        PERSON_HASH_KEY,
        EMAIL_USERNAME_HASH_KEY,
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('sal_person') }}
),
pivot_sal as (
    select 
        *
    from sal
    --PIVOT(MAX(PERSON_HASH_KEY) FOR BUSINESS_KEY_COLLISION_CODE IN ('GSHEETS','RESOURCE_MANAGER','HUBSPOT'))
    PIVOT(MAX(PERSON_HASH_KEY) FOR BUSINESS_KEY_COLLISION_CODE IN ({{ column_values_list }}))
    AS p (EMAIL_USERNAME_HASH_KEY,{{ column_headers_list }} )
    --AS p (EMAIL_USERNAME_HASH_KEY,BIZTORY_TEAM_PERSON_HASH_KEY,RESOURCE_MANAGER_PERSON_HASH_KEY,HUBSPOT_PERSON_HASH_KEY )
)

select * from pivot_sal    