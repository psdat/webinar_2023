{%- set yaml_metadata -%}
source_model:
  twitter: organic_tweet_report
derived_columns:
  TWEET_BUSINESS_KEY: ORGANIC_TWEET_ID
  O_TWEET_BUSINESS_KEY: concat(organic_tweet_id,date)
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!TWITTER'
  RECORD_SOURCE: '!{{ source('twitter', 'organic_tweet_report') }}'
hashed_columns:
  O_TWEET_HASH_KEY:
    - O_TWEET_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  TWEET_HASH_KEY:
    - TWEET_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - O_TWEET_BUSINESS_KEY
      - O_TWEET_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
      - organic_tweet_id
      - TWEET_BUSINESS_KEY
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

