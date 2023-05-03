{%- set source_model = "v_stg_slack__messages" -%}
{%- set src_pk = "MESSAGE_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["LOAD_DATE_TIMESTAMP","BUSINESS_KEY_COLLISION_CODE","_FIVETRAN_SYNCED","MESSAGE_BUSINESS_KEY","PERSON_HASH_KEY","CHANNEL_HASH_KEY","PERSON_BUSINESS_KEY","CHANNEL_BUSINESS_KEY","TS","CHANNEL","USER"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}