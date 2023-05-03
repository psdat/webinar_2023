{%- set yaml_metadata -%}
source_model:
  smartsheet: ops_resource_request_form
derived_columns:
  RESPONSE_BUSINESS_KEY: '_ROW'
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!SMARTSHEET'
  RECORD_SOURCE: '!{{ source('smartsheet', 'ops_resource_request_form') }}'
hashed_columns:
  RESPONSE_HASH_KEY:
    - RESPONSE_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  PROJECT_CODE_HASH_KEY:
    - HUB_SPOT_LINE_ITEM
    - BUSINESS_KEY_COLLISION_CODE
  LINK_RESPONSE_LINE_ITEM_HASH_KEY:
    - RESPONSE_BUSINESS_KEY
    - HUB_SPOT_LINE_ITEM
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - RESPONSE_HASH_KEY
      - RESPONSE_BUSINESS_KEY
      - PROJECT_CODE_HASH_KEY
      - LINK_RESPONSE_LINE_ITEM_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
      - _ROW
      - HUB_SPOT_LINE_ITEM
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