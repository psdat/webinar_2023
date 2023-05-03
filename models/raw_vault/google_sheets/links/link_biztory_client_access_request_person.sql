{%- set source_model = "v_stg_google_sheets__biztory_client_access_request_log" -%}
{%- set src_pk = "LINK_BIZ_CAR_PERSON_HASH_KEY" -%}
{%- set src_fk = ["BIZ_CAR_HASH_KEY", "PERSON_HASH_KEY"] -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{%- set src_extra_columns = ["BUSINESS_KEY_COLLISION_CODE"] -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model, src_extra_columns=src_extra_columns) }}