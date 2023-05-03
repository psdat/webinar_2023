{%- set source_model = ["v_stg_google_sheets__biztory_team","v_stg_resource_manager__users","v_stg_hubspot__owner","v_stg_slack__users","v_stg_hubspot__contact"] -%}
{%- set src_pk = "PERSON_HASH_KEY" -%}
{%- set src_nk = "PERSON_BUSINESS_KEY" -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{%- set src_extra_columns = ["BUSINESS_KEY_COLLISION_CODE"] -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model, src_extra_columns=src_extra_columns) }}