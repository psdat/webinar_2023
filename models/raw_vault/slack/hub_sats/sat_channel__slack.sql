{%- set source_model = "v_stg_slack__channels" -%}
{%- set src_pk = "CHANNEL_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["ID","_FIVETRAN_SYNCED","BUSINESS_KEY_COLLISION_CODE","CHANNEL_BUSINESS_KEY"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}