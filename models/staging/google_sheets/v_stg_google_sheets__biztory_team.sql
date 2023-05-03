{%- set yaml_metadata -%}
source_model:
  google_sheets: biztory_team
derived_columns:
  PERSON_BUSINESS_KEY: regexp_substr(email_address,'.*@.*[a-zA-Z0-9-]{3,}[.]')
  EMAIL_USERNAME_FK: 'NULL'
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!GOOGLE_SHEETS'
  RECORD_SOURCE: '!{{ source('google_sheets', 'biztory_team') }}'
hashed_columns:
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINK_PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - EMAIL_USERNAME_FK
    - BUSINESS_KEY_COLLISION_CODE
  EMAIL_USERNAME_HASH_KEY:
    - PERSON_BUSINESS_KEY
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - PERSON_HASH_KEY
      - PERSON_BUSINESS_KEY
      - LOAD_DATE_TIMESTAMP 
      - _FIVETRAN_SYNCED
      - LINK_PERSON_HASH_KEY
      - EMAIL_USERNAME_HASH_KEY
      - EMAIL
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