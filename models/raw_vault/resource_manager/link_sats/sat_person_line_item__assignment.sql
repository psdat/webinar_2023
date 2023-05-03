{%- set source_model = "v_stg_resource_manager__assignments" -%}
{%- set src_pk = "ASSIGNMENT_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["ID","ASSIGNMENT_BUSINESS_KEY","LINE_ITEM_HASH_KEY","ASSIGNABLE_ID","PERSON_HASH_KEY","USER_ID","_FIVETRAN_SYNCED","PERSON_KEY_COLLISION_CODE","BUSINESS_KEY_COLLISION_CODE"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}