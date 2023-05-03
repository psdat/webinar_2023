{%- set source_model = "v_stg_slack__users" -%}
{%- set src_pk = "PERSON_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["ID","PERSON_BUSINESS_KEY","EMAIL_BUSINESS_KEY","PERSON_BUSINESS_KEY","BUSINESS_KEY_COLLISION_CODE","_FIVETRAN_SYNCED"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
                