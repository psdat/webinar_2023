{%- set yaml_metadata -%}
source_model:
  google_sheets: biztory_projects_internal
derived_columns:
  BIZTORY_PROJECTS_INTERNAL_BUSINESS_KEY: CONCAT(PROJECT_GROUP,PROJECT_NAME)
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!GOOGLE_SHEETS'
  RECORD_SOURCE: '!{{ source('google_sheets', 'biztory_projects_internal') }}'
hashed_columns:
  BIZTORY_PROJECTS_INTERNAL_HASH_KEY:
    - BIZTORY_PROJECTS_INTERNAL_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - BIZTORY_PROJECTS_INTERNAL_BUSINESS_KEY
      - PROJECT_GROUP
      - PROJECT_NAME
      - BIZTORY_PROJECTS_INTERNAL_HASH_KEY
      - LOAD_DATE_TIMESTAMP 
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
