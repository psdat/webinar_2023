{%- set source_model = ["v_stg_google_sheets__biztory_client_access_request_log"] -%}
{%- set src_pk = "BIZ_CAR_HASH_KEY" -%}
{%- set src_nk = "BIZ_CAR_BUSINESS_KEY" -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{%- set src_extra_columns = ["BUSINESS_KEY_COLLISION_CODE"] -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model, src_extra_columns=src_extra_columns) }}