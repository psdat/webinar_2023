{%- set yaml_metadata -%}
source_model:
  slack: users
derived_columns:
  PERSON_BUSINESS_KEY: ID
  EMAIL_BUSINESS_KEY: regexp_substr(profile:email::varchar,'.*@.*[a-zA-Z0-9-]{3,}[.]')
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!SLACK'
  RECORD_SOURCE: '!{{ source('slack', 'users') }}'
hashed_columns:
  EMAIL_USERNAME_HASH_KEY:
    - EMAIL_BUSINESS_KEY
  PERSON_HASH_KEY:
    - PERSON_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - PERSON_HASH_KEY
      - LOAD_DATE_TIMESTAMP  
      - _FIVETRAN_SYNCED
      - BUSINESS_KEY_COLLISION_CODE
      - PERSON_BUSINESS_KEY
      - ID
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{% set source_ldts = '_FIVETRAN_SYNCED' %}


with user as (
{{ stage_deltas_only(include_source_columns=true,
              source_model=source_model,
              derived_columns=derived_columns,
              hashed_columns=hashed_columns,
              ranked_columns=none,
              source_ldts=source_ldts) }}
)

select 
*
from user
where EMAIL_BUSINESS_KEY is not null
