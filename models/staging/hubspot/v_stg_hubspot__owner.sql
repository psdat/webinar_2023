{%- set yaml_metadata -%}
source_model:
  hubspot: owner
derived_columns:
  PERSON_BUSINESS_KEY: OWNER_ID::VARCHAR
  EMAIL_USERNAME_FK: SPLIT_PART(EMAIL,'@',1) || '@' || SPLIT_PART(SPLIT_PART(EMAIL,'@',2),'.',1) || '.'
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!HUBSPOT_OWNER'
  RECORD_SOURCE: '!{{ source('hubspot', 'owner') }}'
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
      - PERSON_HASH_KEY
      - PERSON_BUSINESS_KEY
      - EMAIL_USERNAME_FK_HASH_KEY
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