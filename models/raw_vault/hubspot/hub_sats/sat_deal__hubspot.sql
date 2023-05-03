{%- set source_model = "v_stg_hubspot__deal" -%}
{%- set src_pk = "DEAL_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["LINK_DEAL_PERSON_HASH_KEY","DEAL_BUSINESS_KEY","OWNER_ID","DEAL_ID","_FIVETRAN_SYNCED","BUSINESS_KEY_COLLISION_CODE","DEAL_PIPELINE_STAGE_ID","DEAL_PIPELINE_ID"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}