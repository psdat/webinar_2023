{%- set yaml_metadata -%}
source_model:
  hubspot: line_item
derived_columns:
  LINE_ITEM_BUSINESS_KEY: ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!HUBSPOT'
  RECORD_SOURCE: '!{{ source('hubspot', 'line_item') }}'
hashed_columns:
  LINE_ITEM_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  DEAL_HASH_KEY:
    - DEAL_ID
    - BUSINESS_KEY_COLLISION_CODE
  PRODUCT_HASH_KEY:
    - PRODUCT_ID
    - BUSINESS_KEY_COLLISION_CODE
  LINK_LINE_ITEM_DEAL_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - DEAL_ID
    - BUSINESS_KEY_COLLISION_CODE
  LINK_LINE_ITEM_PRODUCT_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - PRODUCT_ID
    - BUSINESS_KEY_COLLISION_CODE
  SAL_LINE_ITEM_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - ID
      - LINE_ITEM_HASH_KEY
      - LINE_ITEM_BUSINESS_KEY
      - DEAL_HASH_KEY
      - LINK_LINE_ITEM_DEAL_HASH_KEY
      - PRODUCT_HASH_KEY
      - LINK_LINE_ITEM_PRODUCT_HASH_KEY
      - SAL_LINE_ITEM_HASH_KEY
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
