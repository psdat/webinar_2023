{%- set source_model = "v_stg_calendly__doctor_tableau_booking" -%}
{%- set src_pk = "LINK_DR_BOOKING_DEAL_HASH_KEY" -%}
{%- set src_fk = ["DR_BOOKING_HASH_KEY", "DEAL_HASH_KEY"] -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{%- set src_extra_columns = ["BUSINESS_KEY_COLLISION_CODE"] -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model, src_extra_columns=src_extra_columns) }}