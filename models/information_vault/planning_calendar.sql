with calendar as (
    select
        date, 
        day_of_week, 
        week_of_year
    from {{ ref('calendar') }}
),
hub_person_bt as (
    select
        PERSON_HASH_KEY, 
        PERSON_BUSINESS_KEY, 
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('hub_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'GOOGLE_SHEETS'
),
max_sat_person_biztory_team as (
    SELECT
        PERSON_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person__biztory_team') }}
    GROUP BY PERSON_HASH_KEY    
),
sat_person_biztory_team as (
    select
        bt.PERSON_HASH_KEY,
        bt.EMAIL_ADDRESS,
        bt.FULL_NAME,
        bt.team,
        bt.country,
        bt.start_date,
        bt.end_date
    from {{ ref('sat_person__biztory_team') }} bt
    INNER JOIN max_sat_person_biztory_team max_bt_sat
    ON max_bt_sat.MAX_TIMESTAMP = bt.LOAD_DATE_TIMESTAMP
    AND max_bt_sat.PERSON_HASH_KEY = bt.PERSON_HASH_KEY
    where bt.role='Consultant' and status='Active' 
),
sal_person_pivot as (
    select
        EMAIL_USERNAME_HASH_KEY,
        GOOGLE_SHEETS_PERSON_HASH_KEY,
        RESOURCE_MANAGER_PERSON_HASH_KEY,
        HUBSPOT_OWNER_PERSON_HASH_KEY
    from {{ref('sal_person_pivot')}}
)

SELECT
    hub_person.PERSON_HASH_KEY,
    hub_person.PERSON_BUSINESS_KEY,
    sal_person_pivot.EMAIL_USERNAME_HASH_KEY,
    sal_person_pivot.GOOGLE_SHEETS_PERSON_HASH_KEY,
    sal_person_pivot.RESOURCE_MANAGER_PERSON_HASH_KEY,
    sal_person_pivot.HUBSPOT_OWNER_PERSON_HASH_KEY,
    sat_person.email_address,
    sat_person.FULL_NAME,
    sat_person.team,
    sat_person.country,
    cal.date::DATE as date,
    cal.week_of_year
FROM hub_person_bt hub_person
LEFT JOIN sat_person_biztory_team sat_person ON hub_person.PERSON_HASH_KEY = sat_person.PERSON_HASH_KEY
LEFT JOIN sal_person_pivot ON sal_person_pivot.GOOGLE_SHEETS_PERSON_HASH_KEY = hub_person.PERSON_HASH_KEY
    JOIN calendar cal
      ON cal.date >= sat_person.start_date
      AND cal.date <= COALESCE(sat_person.end_date, DATEADD(day, 365, CURRENT_DATE))
WHERE cal.day_of_week > 0 AND cal.day_of_week < 6