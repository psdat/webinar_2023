{%- set yaml_metadata -%}
source_model:
  talent_development: measurements_completion
derived_columns:
  MEASUREMENT_COMPLETION_BUSINESS_KEY: _row
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!TALENT_DEVELOPEMNT'
  RECORD_SOURCE: '!{{ source('talent_development', 'measurements_completion') }}'
hashed_columns:
  MEASUREMENT_COMPLETION_HASH_KEY:
    - MEASUREMENT_COMPLETION_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - MEASUREMENT_COMPLETION_HASH_KEY
      - MEASUREMENT_COMPLETION_BUSINESS_KEY
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

