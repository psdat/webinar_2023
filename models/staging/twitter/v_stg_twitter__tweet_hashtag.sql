{%- set yaml_metadata -%}
source_model:
  twitter: tweet_hashtag
derived_columns:
  TWEET_BUSINESS_KEY: TWEET_ID
  HASHTAG_BUSINESS_KEY: concat(tweet_id,index)
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!TWITTER'
  RECORD_SOURCE: '!{{ source('twitter', 'tweet_hashtag') }}'
hashed_columns:
  HASHTAG_HASH_KEY:
    - HASHTAG_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - HASHTAG_BUSINESS_KEY
      - HASHTAG_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
      - TWEET_BUSINESS_KEY
      - TWEET_ID
      - index
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{% set source_ldts = '_FIVETRAN_SYNCED' %}

{{ stage_deltas_only(include_source_columns=true,
              source_model=source_model,
              derived_columns=derived_columns,
              hashed_columns=hashed_columns,
              ranked_columns=none,
              source_ldts=source_ldts) }}

