{%- set yaml_metadata -%}
source_model:
  hubspot: deal_company
derived_columns:
  COMPANY_BUSINESS_KEY: COMPANY_ID
  DEAL_BUSINESS_KEY: DEAL_ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!HUBSPOT'
  RECORD_SOURCE: '!{{ source('hubspot', 'deal_company') }}'
hashed_columns:
  COMPANY_HASH_KEY:
    - COMPANY_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  DEAL_HASH_KEY:
    - DEAL_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINK_DEAL_COMPANY_HASH_KEY:
    - DEAL_BUSINESS_KEY
    - COMPANY_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - COMPANY_HASH_KEY
      - COMPANY_BUSINESS_KEY
      - DEAL_HASH_KEY
      - DEAL_ID
      - LINK_DEAL_COMPANY_HASH_KEY
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

