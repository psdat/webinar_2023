{%- set yaml_metadata -%}
source_model:
  google_sheets: user_platform_cost_overview
derived_columns:
  PERSON_BUSINESS_KEY: regexp_substr(E_MAIL,'.*@.*[a-zA-Z0-9-]{3,}[.]')
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!GOOGLE_SHEETS'
  RECORD_SOURCE: '!{{ source('google_sheets', 'user_platform_cost_overview') }}'
hashed_columns:
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - PERSON_BUSINESS_KEY
      - PERSON_HASH_KEY
      - E_MAIL
      - USER_FROM_EMAIL
      - LOAD_DATE_TIMESTAMP 
      - _FIVETRAN_SYNCED
      - _ROW
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
