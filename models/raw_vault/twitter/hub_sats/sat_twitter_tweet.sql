{%- set source_model = "v_stg_twitter__tweet" -%}
{%- set src_pk = "TWEET_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["ID","TWEET_BUSINESS_KEY","USER_ID","USER_MENTIONED_BUSINESS_KEY","USER_MENTIONED_HASH_KEY","LINK_USER_MENTIONED_TWEET_HASH_KEY","_FIVETRAN_SYNCED","TWEET_ID","BUSINESS_KEY_COLLISION_CODE"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}