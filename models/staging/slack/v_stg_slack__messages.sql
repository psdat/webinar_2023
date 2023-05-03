{%- set yaml_metadata -%}
source_model:
  slack: messages
derived_columns:
  MESSAGE_BUSINESS_KEY: TS
  CHANNEL_BUSINESS_KEY: CHANNEL
  PERSON_BUSINESS_KEY: USER
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!SLACK'
  RECORD_SOURCE: '!{{ source('slack', 'messages') }}'
hashed_columns:
  CHANNEL_HASH_KEY:
    - CHANNEL_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  MESSAGE_HASH_KEY:
    - MESSAGE_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - TS
      - CHANNEL
      - USER
      - MESSAGE_BUSINESS_KEY
      - MESSAGE_HASH_KEY
      - CHANNEL_BUSINESS_KEY
      - CHANNEL_HASH_KEY
      - PERSON_BUSINESS_KEY
      - PERSON_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
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

