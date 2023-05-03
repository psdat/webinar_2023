{%- set yaml_metadata -%}
source_model:
  twitter: tweet
derived_columns:
  TWEET_BUSINESS_KEY: ID
  USER_MENTIONED_BUSINESS_KEY: IN_REPLY_TO_USER_ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!TWITTER'
  RECORD_SOURCE: '!{{ source('twitter', 'tweet') }}'
hashed_columns:
  TWEET_HASH_KEY:
    - TWEET_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  USER_MENTIONED_HASH_KEY:
    - USER_MENTIONED_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINK_USER_MENTIONED_TWEET_HASH_KEY:
    - USER_MENTIONED_BUSINESS_KEY
    - TWEET_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - TWEET_BUSINESS_KEY
      - TWEET_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
      - ID
      - LINK_USER_MENTIONED_TWEET_HASH_KEY
      - USER_MENTIONED_BUSINESS_KEY
      - USER_MENTIONED_HASH_KEY
      - IN_REPLY_TO_USER_ID
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

