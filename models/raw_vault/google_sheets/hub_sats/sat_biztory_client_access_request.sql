{%- set source_model = "v_stg_google_sheets__biztory_client_access_request_log" -%}
{%- set src_pk = "BIZ_CAR_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["BIZ_CAR_BUSINESS_KEY","PERSON_HASH_KEY","PERSON_BUSINESS_KEY","LINK_BIZ_CAR_PERSON_HASH_KEY","_FIVETRAN_SYNCED","BUSINESS_KEY_COLLISION_CODE"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}