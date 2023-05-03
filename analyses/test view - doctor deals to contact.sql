-- link deal to contact

with hub_deal as (

    select * from {{ ref('hub_deal') }}

),

link_deal_person as (

    select * from {{ ref('link_deal_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),

hub_person as (

    select * from {{ ref('hub_person') }}
),

--- take latest records and join to sat_deal
max_sat_deal as (
        
    select 
    DEAL_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_deal_hubspot') }}
    GROUP BY DEAL_HASH_KEY
    
),

sat_deal as (

  SELECT
        sd.DEAL_HASH_KEY,
        sd.DEAL_ID,
        sd.property_dealname as Deal_Name

    FROM {{ ref('sat_deal_hubspot')}}  sd
    INNER JOIN max_sat_deal msd
    ON msd.MAX_TIMESTAMP = sd.LOAD_DATE_TIMESTAMP
    AND msd.DEAL_HASH_KEY = sd.DEAL_HASH_KEY
    WHERE sd.property_dealname like '%Doctor%'

),

-- take latest contacts and join to deal

max_sat_contacts as (
        
    select 
    PERSON_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_person_hubspot_contacts') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
    GROUP BY PERSON_HASH_KEY

--select * from {{ ref('sat_person_hubspot_contacts') }}
    
),

sat_contacts as (

  SELECT
        sc.PERSON_HASH_KEY,
        sc.ID as Contact_Id,
        sc.property_email as Contact_Email

    FROM {{ ref('sat_person_hubspot_contacts')}}  sc
    INNER JOIN max_sat_contacts msc
    ON msc.MAX_TIMESTAMP = sc.LOAD_DATE_TIMESTAMP
    AND msc.PERSON_HASH_KEY = sc.PERSON_HASH_KEY
    WHERE  sc.property_email like '%biztory%'

)

--- view for business

select 
hub_deal.DEAL_HASH_KEY,
hub_deal.DEAL_BUSINESS_KEY,
link_deal_person.PERSON_HASH_KEY,
hub_person.PERSON_BUSINESS_KEY,
sat_deal.DEAL_ID,
sat_deal.DEAL_NAME,
sat_contacts.Contact_Id,
sat_contacts.Contact_Email


 from hub_deal
 inner join link_deal_person  on hub_deal.DEAL_HASH_KEY = link_deal_person.DEAL_HASH_KEY
 inner join hub_person  on hub_person.PERSON_HASH_KEY = link_deal_person.PERSON_HASH_KEY

 left join sat_deal on hub_deal.DEAL_HASH_KEY = sat_deal.DEAL_HASH_KEY

 left join sat_contacts on hub_person.PERSON_HASH_KEY = sat_contacts.PERSON_HASH_KEY -- no match

 where sat_deal.DEAL_ID is not null

 --select * from {{ ref('hub_person') }}