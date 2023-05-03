-- on the assumption that a deal is enterted into calendly
-- step 2 is to add quarter deals to doctor bookings so we can get client name and contacts


-- link deal to contact

with hub_deal as (

    select * from {{ ref('hub_deal') }}

),
link_deal_person_owner as (

    select * from {{ ref('link_deal_person') }}
    where BUSINESS_KEY_COLLISION_CODE LIKE 'HUBSPOT_OWNER'
),

link_deal_person_contact as (

    select * from {{ ref('link_deal_person') }}
    where BUSINESS_KEY_COLLISION_CODE LIKE 'HUBSPOT_CONTACT'
),

hub_person_owner as (

    select * from {{ ref('hub_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_OWNER'
),

hub_person_contact as (

    select * from {{ ref('hub_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_CONTACT'
),

max_sat_deal as (
        
    select 
    DEAL_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_deal__hubspot') }}
    GROUP BY DEAL_HASH_KEY 

),
sat_deal as (

  SELECT
        sd.DEAL_HASH_KEY,
        sd.property_dealname as Deal_Name,
        sd.property_closedate,
        case when dateadd(year,1, sd.property_closedate) < getdate() then 1 else 0 end AS Deal_Expired


    FROM {{ ref('sat_deal__hubspot')}}  sd
    INNER JOIN max_sat_deal msd
    ON msd.MAX_TIMESTAMP = sd.LOAD_DATE_TIMESTAMP
    AND msd.DEAL_HASH_KEY = sd.DEAL_HASH_KEY

),
max_sat_owner as (
        
    select 
    PERSON_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_person__hubspot_owner') }}
    GROUP BY PERSON_HASH_KEY

),
sat_owner as (

  SELECT
        so.PERSON_HASH_KEY,
        --so.owner_id as OWNER_ID,
        so.email as Owner_Email,
        so.first_name || ' ' || so.last_name as Owner_Name

    FROM {{ ref('sat_person__hubspot_owner')}}  so
    INNER JOIN max_sat_owner mso
    ON mso.MAX_TIMESTAMP = so.LOAD_DATE_TIMESTAMP
    AND mso.PERSON_HASH_KEY = so.PERSON_HASH_KEY
   --WHERE  sc.email  like '%biztory%'

),
max_sat_contacts as (
        
    select 
    PERSON_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_person__hubspot_contact') }}
    GROUP BY PERSON_HASH_KEY

),
sat_contacts as (

  SELECT
        sc.PERSON_HASH_KEY,
        --sc.id as Contact_Id,
        sc.property_email as Contact_Email

    FROM {{ ref('sat_person__hubspot_contact')}}  sc
    INNER JOIN max_sat_contacts msc
    ON msc.MAX_TIMESTAMP = sc.LOAD_DATE_TIMESTAMP
    AND msc.PERSON_HASH_KEY = sc.PERSON_HASH_KEY
   -- WHERE  sc.property_email not like '%biztory%'

)

select 
sat_deal.*,
hub_deal.deal_business_key,
sat_owner.*,
sat_contacts.*

 from hub_deal

inner join link_deal_person_contact on hub_deal.DEAL_HASH_KEY = link_deal_person_contact.DEAL_HASH_KEY
inner join link_deal_person_owner on hub_deal.DEAL_HASH_KEY = link_deal_person_owner.DEAL_HASH_KEY

left join hub_person_owner on link_deal_person_owner.person_hash_key = hub_person_owner.person_hash_key
left join hub_person_contact on link_deal_person_contact.PERSON_HASH_KEY = hub_person_contact.PERSON_hash_KEY 
inner join sat_deal on hub_deal.DEAL_HASH_KEY = sat_deal.DEAL_HASH_KEY
left join sat_owner on hub_person_owner.person_hash_key = sat_owner.person_hash_key
left join sat_contacts on hub_person_contact.PERSON_HASH_KEY = sat_contacts.person_hash_key


where deal_business_key = 1133680158