with rm_users_hub as (
    select
        PERSON_HASH_KEY, 
        PERSON_BUSINESS_KEY,
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('hub_person')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
),
rm_users_hub_sat_max as (
    SELECT
        PERSON_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_person__resource_manager')}}
    GROUP BY PERSON_HASH_KEY
),
rm_users_hub_sat as (
    SELECT
        rm_s.PERSON_HASH_KEY,
        rm_s.archived,
        rm_s.deleted
    FROM {{ ref('sat_person__resource_manager')}}  rm_s
    INNER JOIN rm_users_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_s.LOAD_DATE_TIMESTAMP
    AND max_sat.PERSON_HASH_KEY = rm_s.PERSON_HASH_KEY
)

select
    uh.PERSON_HASH_KEY,
    uh.PERSON_BUSINESS_KEY,
    uh.BUSINESS_KEY_COLLISION_CODE
from rm_users_hub uh
INNER JOIN rm_users_hub_sat uhs ON uhs.PERSON_HASH_KEY=uh.PERSON_HASH_KEY
WHERE uhs.archived = FALSE and uhs.deleted = FALSE