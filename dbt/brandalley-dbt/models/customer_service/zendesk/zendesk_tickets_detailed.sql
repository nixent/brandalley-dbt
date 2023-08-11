{{ config(
	materialized='table',
	unique_key='id'
) }}

with tickets as (
    select  ticket.id,
            ticket.created_at,
            ticket.description,
            ticket.due_at,
            ticket.is_public,
            ticket.merged_ticket_ids,
            ticket.status,
            ticket.subject,
            ticket.updated_at,
            ticket.type,
            ticket.url,
            ticket.via_channel,
            ticket.via_source_rel,
            if(ticket.via_channel='voice', 1, 0) as phone_ticket,
            if(ticket.via_channel='native_messaging', 1, 0) as chat_ticket,
            if(ticket.via_channel in ('web','email'), 1, 0) as web_email_ticket,
            if(ticket.via_channel='web', 1, 0) as web_ticket,
            if(ticket.via_channel='email', 1, 0) as email_ticket,
            ticket.custom_carrier,
            ticket.custom_client_contact_reason_no_order,
            ticket.custom_client_contact_reason_order,
            ticket.custom_contact_reason,
            ticket.custom_coupon_code,
            ticket.custom_needs_senior_help,
            ticket.custom_order_id,
            ticket.custom_order_number,
            ticket.custom_order_related,
            ticket.custom_product_issue_type,
            ticket.custom_shipping_issue_type,
            ticket.custom_solution,
            ticket.custom_status_id,
            ticket.custom_tracking_,
            orderlines.consignment_qty, 
            orderlines.warehouse_qty, 
            orderlines.selffulfill_qty,
            orderlines.order_id,
            carrier_codes,
            title,
            orderlines.split_consignment_units,
            orderlines.split_sf_units
    from {{ source(
        'zendesk',
        'ticket'
    ) }} ticket
    left outer join (
        select  sum(consignment_qty)                    as consignment_qty, 
                sum(warehouse_qty)                      as warehouse_qty, 
                sum(selffulfill_qty)                    as selffulfill_qty,
                count(if(consignment_qty>0, sku, null)) as split_consignment_units, 
                count(if(selffulfill_qty>0, sku, null)) as split_sf_units, 
                order_number, order_id
        from {{ ref('OrderLines') }} 
        -- At the moment filtering on UK only but we'll need to add join on site when FR data is in
        where ba_site='UK'
        group by order_number, order_id
    ) orderlines
        on ifnull(ticket.custom_order_id, ticket.custom_order_number) = orderlines.order_number
    left outer join (
        select 
            string_agg(distinct carrier_code) as carrier_codes, 
            string_agg(distinct title) as title, 
            order_id 
        from {{ ref('stg__sales_flat_shipment_track') }}
        group by 3
    ) track
        on orderlines.order_id = track.order_id
	where 1=1
/*	{% if is_incremental() %}
		and updated_at >= '{{min_ts}}'
	{% endif %}*/
)

select * from tickets