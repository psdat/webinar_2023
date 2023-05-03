with planning_calendar as (
    select 
        EMAIL_USERNAME_HASH_KEY,
        GOOGLE_SHEETS_PERSON_HASH_KEY,
        RESOURCE_MANAGER_PERSON_HASH_KEY,
        HUBSPOT_OWNER_PERSON_HASH_KEY,
        email_address,
        FULL_NAME,
        team,
        country,
        date,
        week_of_year
    from {{ ref('planning_calendar')}}
),
rm_assignments_link as (
    select
        ASSIGNMENT_HASH_KEY,
        LINE_ITEM_HASH_KEY,
        PERSON_HASH_KEY
    from {{ ref('link_person_line_item')}}
),
rm_assignments_link_sat_max as (
    SELECT
        ASSIGNMENT_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person_line_item__assignment')}}
    GROUP BY ASSIGNMENT_HASH_KEY
),
rm_assignments_link_sat as (
    SELECT
        rm_a.ASSIGNMENT_HASH_KEY,
        rm_a.starts_at,
        rm_a.ends_at,
        rm_a.description,
        rm_a.hours_per_day,
        rm_a.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_person_line_item__assignment')}}  rm_a
    INNER JOIN rm_assignments_link_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_a.LOAD_DATE_TIMESTAMP
    AND max_sat.ASSIGNMENT_HASH_KEY = rm_a.ASSIGNMENT_HASH_KEY
),
rm_assignments_link_detail as (
    select
        rm_al.ASSIGNMENT_HASH_KEY,
        rm_al.LINE_ITEM_HASH_KEY,
        rm_al.PERSON_HASH_KEY,
        rm_als.starts_at,
        rm_als.ends_at,
        rm_als.description,
        rm_als.hours_per_day,
        rm_als.LOAD_DATE_TIMESTAMP
    from rm_assignments_link rm_al
    join rm_assignments_link_sat rm_als
    ON rm_als.ASSIGNMENT_HASH_KEY=rm_al.ASSIGNMENT_HASH_KEY
),
rm_phases_hub as (
    SELECT
        LINE_ITEM_HASH_KEY,
        LINE_ITEM_BUSINESS_KEY,
        BUSINESS_KEY_COLLISION_CODE
    FROM {{ ref('hub_line_item')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'    
),
rm_phases_hub_sat_max as (
    SELECT
        LINE_ITEM_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_line_item__resource_manager')}}
    GROUP BY LINE_ITEM_HASH_KEY
),
rm_phases_hub_sat as (
    SELECT
        rm_p.LINE_ITEM_HASH_KEY,
        rm_p.project_code,
        rm_p.project_state,
        rm_p.name,
        rm_p.description,
        rm_p.starts_at,
        rm_p.ends_at,
        rm_p.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_line_item__resource_manager')}}  rm_p
    INNER JOIN rm_phases_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_p.LOAD_DATE_TIMESTAMP
    AND max_sat.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
),
active_rm_users as (
    select
        PERSON_HASH_KEY
    from {{ ref('active_rm_users_hub')}}
),
sal_line_item_pivot as (
    SELECT
        SAL_LINE_ITEM_HASH_KEY,
        RESOURCE_MANAGER_LINE_ITEM_HASH_KEY,
        HUBSPOT_LINE_ITEM_HASH_KEY
    from {{ref('sal_line_item_pivot')}}
)

SELECT 
	cal.email_address AS consultant_email,
	cal.full_name AS consultant,
	cal.date AS engagement_date,
	rm_ps.name AS planned,
	rm_ps.project_code,
	rm_ps.project_state AS engagement_status,
	SPLIT_PART(rm_ps.description, ' | ',1) AS hubspot_deal_id,
	SPLIT_PART(rm_ps.description, ' | ',2) AS hubspot_line_item_id,
    SPLIT_PART(rm_ps.description, ' | ',3) AS hubspot_line_item_name,
	COALESCE(rm_al.hours_per_day, 8) AS hours, -- Default to 8
	rm_al.description AS rm_assignment_description,
    rm_p.LINE_ITEM_HASH_KEY,
    sal_line_item_pivot.HUBSPOT_LINE_ITEM_HASH_KEY,
    cal.HUBSPOT_OWNER_PERSON_HASH_KEY,
    cal.GOOGLE_SHEETS_PERSON_HASH_KEY
FROM planning_calendar cal
    INNER JOIN active_rm_users a_rm_u ON cal.RESOURCE_MANAGER_PERSON_HASH_KEY = a_rm_u.PERSON_HASH_KEY
    left JOIN rm_assignments_link_detail rm_al
        ON cal.date >= rm_al.starts_at AND cal.date <= rm_al.ends_at
        AND rm_al.PERSON_HASH_KEY = a_rm_u.PERSON_HASH_KEY
    left JOIN rm_phases_hub rm_p ON rm_al.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
    left JOIN rm_phases_hub_sat rm_ps ON rm_ps.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
    left join sal_line_item_pivot on sal_line_item_pivot.RESOURCE_MANAGER_LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY