with doctor_bookings as (
    select
        *
    from {{ source('calendly', 'doctor_tableau_booking') }}
),
booking_deal_quarters as (
    select
        *
    from {{ source('calendly', 'booking_deal_quarter') }}
)

select
    drb.EVENT_ID,
    drb.START_TIME,
    drb.BOOKED_BY,
    drb.COMMENTS,
    drb._FIVETRAN_BATCH,
    drb._FIVETRAN_INDEX,
    drb.BOOKED_BY_EMAIL,
    drb.END_TIME,
    drb.NAME,
    drb.CREATED_AT,
    COALESCE(drb.DEAL_ID, bdq.deal_id)::NUMBER as DEAL_ID,
    drb.STATUS,
    drb._FIVETRAN_SYNCED
from doctor_bookings drb
left join booking_deal_quarters bdq on bdq.deal_quarter = quarter(drb.start_time) and drb.name = 'Janssen Tableau Doctor on Call'