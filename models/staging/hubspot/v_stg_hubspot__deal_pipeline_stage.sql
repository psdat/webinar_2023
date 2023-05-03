{%- set yaml_metadata -%}
source_model:
  hubspot: deal_pipeline_stage
derived_columns:
  DEAL_PIPELINE_STAGE_BUSINESS_KEY: CONCAT(PIPELINE_ID,STAGE_ID)
  DEAL_PIPELINE_BUSINESS_KEY: PIPELINE_ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!HUBSPOT'
  RECORD_SOURCE: '!{{ source('hubspot', 'deal_pipeline_stage') }}'
hashed_columns:
  DEAL_PIPELINE_STAGE_HASH_KEY:
    - DEAL_PIPELINE_STAGE_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  DEAL_PIPELINE_HASH_KEY:
    - DEAL_PIPELINE_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - PIPELINE_ID
      - STAGE_ID
      - DEAL_PIPELINE_STAGE_HASH_KEY
      - DEAL_PIPELINE_HASH_KEY
      - DEAL_PIPELINE_STAGE_BUSINESS_KEY
      - DEAL_PIPELINE_BUSINESS_KEY
      - LOAD_DATE_TIMESTAMP
      - BUSINESS_KEY_COLLISION_CODE
      - _FIVETRAN_SYNCED
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
