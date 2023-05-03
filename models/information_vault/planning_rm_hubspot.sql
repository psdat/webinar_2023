with planning_rm as (
    select *
    from {{ ref('planning_rm')}}
),
hub_line_item as (
    select *
    from {{ ref('hub_line_item')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hubspot_line_item_hub_sat_max as (
    SELECT
        LINE_ITEM_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_line_item__hubspot')}}
    GROUP BY LINE_ITEM_HASH_KEY
),
hubspot_line_item_hub_sat as (
    SELECT
        hli.LINE_ITEM_HASH_KEY,
        hli.property_price,
        hli.property_quantity,
        hli.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_line_item__hubspot')}}  hli
    INNER JOIN hubspot_line_item_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = hli.LOAD_DATE_TIMESTAMP
    AND max_sat.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
),
link_line_item_deal as (
    select *
    from {{ ref('link_line_item_deal')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_deal as (
    select *
    from {{ ref('hub_deal')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
link_deal_company as (
    select *
    from {{ ref('link_deal_company')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_company as (
    select *
    from {{ ref('hub_company')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hubspot_company_hub_sat_max as (
    SELECT
        COMPANY_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_company')}}
    GROUP BY COMPANY_HASH_KEY
),
hubspot_company_hub_sat as (
    SELECT
        hc.COMPANY_HASH_KEY,
        hc.property_name,
        hc.property_biztory_shortcode,
        hc.property_address,
        hc.property_city,
        hc.property_zip,
        hc.property_state,
        hc.property_country
    FROM {{ ref('sat_company')}}  hc
    INNER JOIN hubspot_company_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = hc.LOAD_DATE_TIMESTAMP
    AND max_sat.COMPANY_HASH_KEY = hc.COMPANY_HASH_KEY
),
link_deal_person_owner as (
    select *
    from {{ ref('link_deal_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_OWNER'
),
hub_person_owner as (
    select *
    from {{ ref('hub_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_OWNER'
),

link_deal_person_contact as (
    select *
    from {{ ref('link_deal_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_CONTACT'
),
hub_person_contact as (
    select *
    from {{ ref('hub_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_CONTACT'
),
sat_owner_max as (
    SELECT
        PERSON_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person__hubspot_owner')}}
    GROUP BY PERSON_HASH_KEY
),
sat_owner as (
    SELECT
        so.PERSON_HASH_KEY,
        so.first_name || ' ' || so.last_name as Owner_Name,
        so.email as Owner_Email
    FROM {{ ref('sat_person__hubspot_owner')}}  so
    INNER JOIN sat_owner_max mso
    ON mso.MAX_TIMESTAMP = so.LOAD_DATE_TIMESTAMP
    AND mso.PERSON_HASH_KEY = so.PERSON_HASH_KEY
),

sat_contact_max as (
    SELECT
        PERSON_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person__hubspot_contact')}}
    GROUP BY PERSON_HASH_KEY
),
sat_contact as (
    SELECT
        sc.PERSON_HASH_KEY,
        sc.property_firstname || ' ' || sc.property_lastname as Contact_Name,
        sc.property_email as Contact_Email
    FROM {{ ref('sat_person__hubspot_contact')}}  sc
    INNER JOIN sat_contact_max max_sc
    ON max_sc.MAX_TIMESTAMP = sc.LOAD_DATE_TIMESTAMP
    AND max_sc.PERSON_HASH_KEY = sc.PERSON_HASH_KEY
),
hub_person_bt as (
    select *
    from {{ ref('hub_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'GOOGLE_SHEETS'
),
bt_person_hub_sat_max as (
    SELECT
        PERSON_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person__biztory_team')}}
    GROUP BY PERSON_HASH_KEY
),
bt_person_hub_sat as (
    SELECT
        bts.PERSON_HASH_KEY,
        bts.full_name,
        bts.reports_to,
        bts.start_date,
        bts.end_date,
        bts.cronos_login,
        bts.team,
        bts.biztory_branch,
        bts.status,
        bts.member_type,
        bts.role,
        bts.country
    FROM {{ ref('sat_person__biztory_team')}}  bts
    INNER JOIN bt_person_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = bts.LOAD_DATE_TIMESTAMP
    AND max_sat.PERSON_HASH_KEY = bts.PERSON_HASH_KEY
)


SELECT
    bp.consultant_email,
    hsbts.full_name AS consultant, 
    bp.engagement_date,
    bp.planned,
    bp.engagement_status,
    bp.hours,
    bp.rm_assignment_description,
    bp.project_code,
    hlis.property_price AS bill_rate,
    hlis.property_quantity AS line_item_num_days,
    hd.deal_business_key AS planned_deal_id,
    hc.COMPANY_business_key AS planned_company_id,
    hcs.property_name AS planned_company_name,
    LOWER(hcs.property_biztory_shortcode) AS planned_biztory_shortcode,
    hcs.property_address AS planned_company_address,
    hcs.property_city AS planned_company_city,
    hcs.property_zip AS planned_company_zip,
    hcs.property_state AS planned_company_state,
    hcs.property_country AS planned_company_country,
    
    hpos.Owner_Email AS planned_deal_owner,
    hpcs.Contact_Name as contact_name,
    hpcs.Contact_Email as contact_email,
    -- From Team
    hsbts.reports_to AS consultant_reports_to,
    hsbts.start_date AS consultant_start_date,
    hsbts.end_date AS consultant_end_date,
    hsbts.cronos_login AS consultant_cronos_login,
    hsbts.team AS consultant_team,
    hsbts.biztory_branch AS consultant_biztory_branch,
    hsbts.status AS consultant_status,
    hsbts.member_type AS consultant_member_type,
    hsbts.role AS consultant_role,
    hsbts.country AS consultant_country
FROM planning_rm bp
    LEFT JOIN hub_line_item hli ON hli.LINE_ITEM_HASH_KEY = bp.HUBSPOT_LINE_ITEM_HASH_KEY
    LEFT JOIN hubspot_line_item_hub_sat hlis ON hlis.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
    LEFT JOIN link_line_item_deal llid ON llid.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
    LEFT JOIN hub_deal hd ON hd.DEAL_HASH_KEY = llid.DEAL_HASH_KEY
    LEFT JOIN link_deal_company ldc ON ldc.DEAL_HASH_KEY = hd.DEAL_HASH_KEY
    LEFT JOIN hub_company hc ON hc.COMPANY_HASH_KEY = ldc.COMPANY_HASH_KEY
    LEFT JOIN hubspot_company_hub_sat hcs ON hcs.COMPANY_HASH_KEY = hc.COMPANY_HASH_KEY

    LEFT JOIN link_deal_person_owner ldo ON ldo.DEAL_HASH_KEY = hd.DEAL_HASH_KEY
    LEFT JOIN hub_person_owner hpo ON hpo.PERSON_HASH_KEY = ldo.PERSON_HASH_KEY
    LEFT JOIN link_deal_person_contact ldpc ON ldpc.DEAL_HASH_KEY = hd.DEAL_HASH_KEY
    LEFT JOIN hub_person_contact hpc ON hpc.PERSON_HASH_KEY = ldpc.PERSON_HASH_KEY

    LEFT JOIN sat_owner hpos ON hpos.PERSON_HASH_KEY = hpo.PERSON_HASH_KEY
    LEFT JOIN sat_contact hpcs ON hpcs.PERSON_HASH_KEY = hpc.PERSON_HASH_KEY

    LEFT JOIN hub_person_bt hsbt ON hsbt.PERSON_HASH_KEY = bp.GOOGLE_SHEETS_PERSON_HASH_KEY
    LEFT JOIN bt_person_hub_sat hsbts ON hsbts.PERSON_HASH_KEY = hsbt.PERSON_HASH_KEY
