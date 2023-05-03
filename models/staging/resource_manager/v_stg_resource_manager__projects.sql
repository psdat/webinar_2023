{%- set yaml_metadata -%}
source_model:
  resource_manager: projects_phases
derived_columns:
  DEAL_BUSINESS_KEY: ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!RESOURCE_MANAGER'
  RECORD_SOURCE: '!{{ source('resource_manager', 'projects_phases') }}'
hashed_columns:
  DEAL_HASH_KEY:
    - DEAL_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - DEAL_HASH_KEY
      - DEAL_BUSINESS_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{% set source_ldts = '_FIVETRAN_SYNCED' %}

with staging as (
{{ stage_deltas_only(include_source_columns=true,
              source_model=source_model,
              derived_columns=derived_columns,
              hashed_columns=hashed_columns,
              ranked_columns=none,
              source_ldts=source_ldts) }}
)
              
select *
from staging
where PARENT_ID IS NULL