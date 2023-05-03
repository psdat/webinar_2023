{%- set source_model = "v_stg_hubspot__deal_pipeline_stage" -%}
{%- set src_pk = "DEAL_PIPELINE_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["PIPELINE_ID","STAGE_ID","DEAL_PIPELINE_BUSINESS_KEY","DEAL_PIPELINE_STAGE_BUSINESS_KEY","DEAL_PIPELINE_HASH_KEY","DEAL_PIPELINE_STAGE_HASH_KEY","_FIVETRAN_SYNCED","BUSINESS_KEY_COLLISION_CODE"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}