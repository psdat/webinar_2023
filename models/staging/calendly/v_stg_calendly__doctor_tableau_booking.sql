{%- set yaml_metadata -%}
source_model:
  v_base_calendly__doctor_tableau_booking
derived_columns:
  DR_BOOKING_BUSINESS_KEY: EVENT_ID
  DEAL_BUSINESS_KEY: DEAL_ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!CALENDLY'
  RECORD_SOURCE: '!{{ source('calendly', 'doctor_tableau_booking') }}'
hashed_columns:
  DR_BOOKING_HASH_KEY:
    - DR_BOOKING_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  DEAL_HASH_KEY:
    - DEAL_BUSINESS_KEY
    - '!HUBSPOT'
  LINK_DR_BOOKING_DEAL_HASH_KEY:
    - DR_BOOKING_BUSINESS_KEY
    - DEAL_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - DR_BOOKING_HASH_KEY
      - DR_BOOKING_BUSINESS_KEY
      - DEAL_HASH_KEY
      - DEAL_BUSINESS_KEY
      - LINK_DR_BOOKING_DEAL_HASH_KEY
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

