{%- set yaml_metadata -%}
source_model:
  google_sheets: biz_client_access_req_log
derived_columns:
  BIZ_CAR_BUSINESS_KEY: concat(email_address,client_shortcode,timestamp)
  PERSON_BUSINESS_KEY: regexp_substr(email_address,'.*@.*[a-zA-Z0-9-]{3,}[.]')
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!GOOGLE_SHEETS'
  RECORD_SOURCE: '!{{ source('google_sheets', 'biz_client_access_req_log') }}'
hashed_columns:
  BIZ_CAR_HASH_KEY:
    - BIZ_CAR_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINK_BIZ_CAR_PERSON_HASH_KEY:
    - BIZ_CAR_BUSINESS_KEY
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - BIZ_CAR_HASH_KEY
      - BIZ_CAR_BUSINESS_KEY
      - LINK_BIZ_CAR_PERSON_HASH_KEY
      - LOAD_DATE_TIMESTAMP 
      - _FIVETRAN_SYNCED
      - PERSON_HASH_KEY
      - PERSON_BUSINESS_KEY
      - email_address
      - client_shortcode
      - timestamp
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
biz_client_access_req_log