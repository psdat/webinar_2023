{%- set yaml_metadata -%}
source_model:
  talent_development: participant_metadata
derived_columns:
  PERSON_BUSINESS_KEY: concat(PARTICIPANT_NAME,'@biztory.')
  EMAIL_USERNAME_FK: concat(PARTICIPANT_NAME,'@biztory.')
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!TALENT_DEVELOPEMNT'
  RECORD_SOURCE: '!{{ source('talent_development', 'participant_metadata') }}'
hashed_columns:
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  EMAIL_USERNAME_HASH_KEY:
    - EMAIL_USERNAME_FK
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - EMAIL_USERNAME_HASH_KEY
      - EMAIL_USERNAME_FK
      - PARTICIPANT_NAME
      - PERSON_HASH_KEY
      - PERSON_BUSINESS_KEY
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