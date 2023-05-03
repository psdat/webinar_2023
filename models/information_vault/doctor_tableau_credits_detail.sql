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

hub_person_contact as (
    select * from {{ ref('hub_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_CONTACT'
),

hub_person_owner as (

    select * from {{ ref('hub_person') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT_OWNER'


),


-- link deal to company

link_deal_company as (

    select * from {{ ref('link_deal_company') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'

),

hub_company as (

    select * from {{ ref('hub_company') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),

-- link lineitem to deal

link_deal_line_item as (

    select * from {{ ref('link_line_item_deal') }} --only interested in line items with deal
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'

),

hub_line_item as (

    select * from {{ ref('hub_line_item') }}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),

--- take latest records and join to sat_deal
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

--- take latest records and join to sat_company
max_company_deal as (
        
    select 
    COMPANY_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_company') }}
    GROUP BY COMPANY_HASH_KEY 

),
sat_company as (

  SELECT
        scp.COMPANY_HASH_KEY,
        --scp.id as Client_Id,
        scp.property_name as Client_Name,
        scp.property_country as Client_Country,
        scp.property_industry as Client_Industry,
        scp.property_city as Client_City
        
    FROM {{ ref('sat_company')}}  scp
    INNER JOIN max_company_deal mcpd
    ON mcpd.MAX_TIMESTAMP = scp.LOAD_DATE_TIMESTAMP
    AND mcpd.COMPANY_HASH_KEY = scp.COMPANY_HASH_KEY

),

--- take latest records and join to sat_line_item_hubspot
max_line_item_hubspot as (
        
    select 
    LINE_ITEM_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_line_item__hubspot') }}
   -- WHERE property_product like '%Doctor%' -- looks like only uk use this process
    GROUP BY LINE_ITEM_HASH_KEY

),
sat_line_item_hubspot as (

  SELECT
        lih.LINE_ITEM_HASH_KEY,
        --lih.id AS Line_Item_Id,
        lih.property_name AS Product_Name,
        lih.property_description AS Product_Description,
        lih.property_product AS Product_Category,
        lih.property_end_date AS Deal_End_Date,
        lih.property_quantity as Credits_Total
        
    FROM {{ ref('sat_line_item__hubspot')}}  lih
    INNER JOIN max_line_item_hubspot mlih
    ON mlih.MAX_TIMESTAMP = lih.LOAD_DATE_TIMESTAMP
    AND mlih.LINE_ITEM_HASH_KEY = lih.LINE_ITEM_HASH_KEY
    where is_deleted = false

),

-- take latest contacts and join to deal
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
        sc.property_firstname || ' ' || sc.property_lastname as Contact_Name,
        sc.property_email as Contact_Email

    FROM {{ ref('sat_person__hubspot_contact')}}  sc
    INNER JOIN max_sat_contacts msc
    ON msc.MAX_TIMESTAMP = sc.LOAD_DATE_TIMESTAMP
    AND msc.PERSON_HASH_KEY = sc.PERSON_HASH_KEY
   -- WHERE  sc.property_email not like '%biztory%'

),

-- take latest owner and join to deal
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
        so.email as Owner_Email,
        so.first_name || ' ' || so.last_name as Owner_Name

    FROM {{ ref('sat_person__hubspot_owner')}}  so
    INNER JOIN max_sat_owner mso
    ON mso.MAX_TIMESTAMP = so.LOAD_DATE_TIMESTAMP
    AND mso.PERSON_HASH_KEY = so.PERSON_HASH_KEY
  

),

-- take latest doctor booking and join to deal + contact
max_doctor_booking as (
        
    select 
    DR_BOOKING_HASH_KEY,
    MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP

    from {{ ref('sat_doctor_booking') }}
    GROUP BY DR_BOOKING_HASH_KEY

),
sat_doctor_booking as (

  SELECT
        sdb.DR_BOOKING_HASH_KEY,
        --sdb.id AS Event_Id,
        sdb.start_time,
        sdb.end_time,
        sdb.comments,
        sdb.status AS Booking_Status,
        1 as Credits_Used
 
      

    FROM {{ ref('sat_doctor_booking')}}  sdb
    INNER JOIN max_doctor_booking mdb
    ON mdb.MAX_TIMESTAMP = sdb.LOAD_DATE_TIMESTAMP
    AND mdb.DR_BOOKING_HASH_KEY = sdb.DR_BOOKING_HASH_KEY
    WHERE sdb.status = 'Active' -- we are not interested in cancelled calls for this view
     

),
hub_doctor_booking as (
    select * from {{ ref('hub_doctor_booking')}}
),
link_doctor_booking_deal as (
    select * from {{ ref('link_doctor_booking_deal')}}
),

calendar as (

    select date,
    quarter(date) as quarter,
    week_of_year,
    day_of_week,
    month_name || ' ' || to_varchar(year) as period
    from {{ ref('calendar') }}

    where year = year(getdate())
)

--- view for business

select 

hub_deal.DEAL_BUSINESS_KEY as DEAL_ID,

sat_deal.Deal_Name,
sat_deal.Deal_Expired,
sat_deal.Deal_Expired AS Is_Expired,

hub_company.company_business_key as client_id,
sat_company.Client_Name,
sat_company.Client_Industry,
sat_company.Client_Country,
sat_company.Client_City,

sat_line_item_hubspot.Product_Category,
sat_line_item_hubspot.Product_Name,
sat_line_item_hubspot.Product_Description,
sat_line_item_hubspot.Deal_End_Date,
sat_line_item_hubspot.Credits_Total,

hub_person_contact.person_business_key as contact_id,
sat_contacts.Contact_Name,
sat_contacts.Contact_Email,

hub_person_owner.person_business_key as owner_id,
sat_owner.Owner_Email,
sat_owner.Owner_Name,

--had to comment as deal business key is no longer in satellite, link table needs to be created to join these
sat_doctor_booking.start_time,
sat_doctor_booking.end_time,
sat_doctor_booking.comments


 from 
 hub_deal
  -- add deal detail
 left join sat_deal on hub_deal.DEAL_HASH_KEY = sat_deal.DEAL_HASH_KEY

 -- create a link between deal and person (contact + owner)
 left join link_deal_person_owner on hub_deal.DEAL_HASH_KEY = link_deal_person_owner.DEAL_HASH_KEY
 left join link_deal_person_contact on hub_deal.DEAL_HASH_KEY = link_deal_person_contact.DEAL_HASH_KEY

 left join hub_person_owner  on link_deal_person_owner.PERSON_HASH_KEY =  hub_person_owner.PERSON_HASH_KEY
 left join hub_person_contact  on link_deal_person_contact.PERSON_HASH_KEY = hub_person_contact.PERSON_HASH_KEY
-- add contact detail 
 left join sat_contacts on hub_person_contact.PERSON_HASH_KEY = sat_contacts.PERSON_HASH_KEY 
-- add owner detail (who owns the deal and client)
 left join sat_owner on hub_person_owner.PERSON_HASH_KEY = sat_owner.PERSON_HASH_KEY 

 -- create a link between deal and company
 left join link_deal_company on hub_deal.DEAL_HASH_KEY = link_deal_company.DEAL_HASH_KEY
 left join hub_company on hub_company.COMPANY_HASH_KEY = link_deal_company.COMPANY_HASH_KEY
 -- add company detail
 left join sat_company on hub_company.COMPANY_HASH_KEY = sat_company.COMPANY_HASH_KEY

  -- create a link between deal and product
 left join link_deal_line_item on hub_deal.DEAL_HASH_KEY = link_deal_line_item.DEAL_HASH_KEY
 left join hub_line_item on hub_line_item.LINE_ITEM_HASH_KEY = link_deal_line_item.LINE_ITEM_HASH_KEY
-- add product detail
 left join sat_line_item_hubspot on hub_line_item.LINE_ITEM_HASH_KEY = sat_line_item_hubspot.LINE_ITEM_HASH_KEY

-- create link between doctor booking and deal
 left join link_doctor_booking_deal on hub_deal.DEAL_HASH_KEY = link_doctor_booking_deal.DEAL_HASH_KEY
 --left join sat_doctor_booking on hub_deal.deal_business_key = sat_doctor_booking.deal_business_key 
 left join hub_doctor_booking on hub_doctor_booking.DR_BOOKING_HASH_KEY = link_doctor_booking_deal.DR_BOOKING_HASH_KEY
 left join sat_doctor_booking on hub_doctor_booking.DR_BOOKING_HASH_KEY = sat_doctor_booking.DR_BOOKING_HASH_KEY

-- scafold dates for doctor calendar
--right join calendar on to_date(sat_doctor_booking.start_time) = to_date(calendar.date)

where Product_Category like '%Doctor%' or Deal_Name like '%Doctor%'

