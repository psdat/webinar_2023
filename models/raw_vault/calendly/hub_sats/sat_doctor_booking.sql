{%- set source_model = "v_stg_calendly__doctor_tableau_booking" -%}
{%- set src_pk = "DR_BOOKING_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["EVENT_ID","DEAL_ID","DR_BOOKING_BUSINESS_KEY","DEAL_BUSINESS_KEY","DEAL_HASH_KEY","LINK_DR_BOOKING_DEAL_HASH_KEY","_FIVETRAN_SYNCED","BUSINESS_KEY_COLLISION_CODE"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}