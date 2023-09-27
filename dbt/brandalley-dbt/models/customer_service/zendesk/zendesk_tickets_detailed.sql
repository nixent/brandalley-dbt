{{ config(
	materialized='table',
	unique_key='id'
) }}

with tickets as (
    select  ticket.id,
            left(ticket.id, 2) as ba_site,
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
            orders.consignment_qty, 
            orders.warehouse_qty, 
            orders.selffulfill_qty,
            orders.order_id,
            carrier_codes,
            title,
            orders.split_consignment_units,
            orders.split_sf_units,
            orders.brand,
            orders.dispatch_due_date,
            orders.expected_delivery_date,
            orders.shipment_date,
            orders.past_dispatch_date,
            orders.past_delivery_date
    from {{ source(
        'zendesk',
        'ticket'
    ) }} ticket
    left outer join (
        select  sum(ol.consignment_qty)                                                                     as consignment_qty, 
                sum(ol.warehouse_qty)                                                                       as warehouse_qty, 
                sum(ol.selffulfill_qty)                                                                     as selffulfill_qty,
                count(if(ol.consignment_qty>0, ol.sku, null))                                               as split_consignment_units, 
                count(if(ol.selffulfill_qty>0, ol.sku, null))                                               as split_sf_units, 
                string_agg(distinct ol.brand)                                                               as brand,
                max(ol.dispatch_due_date)                                                                   as dispatch_due_date,
                o.expected_delivery_date                                                                    as expected_delivery_date,
                max(s.max_shipment_date)                                                                    as shipment_date,
                max(ol.dispatch_due_date) < IFNULL(DATE(max(s.max_shipment_date)), current_date)                 as past_dispatch_date,
                o.expected_delivery_date < IFNULL(DATE(max(s.max_shipment_date)), current_date)                  as past_delivery_date,
                ol.order_number, ol.order_id, ol.ba_site
        from {{ ref('OrderLines') }} ol
        left outer join {{ ref('Orders') }} o
        on o.order_id=ol.order_id and o.ba_site=ol.ba_site
        left outer join (select max(shipment_date) max_shipment_date, order_id, sku, ba_site from {{ ref('shipping')}} group by order_id, ba_site, sku) s
        on o.increment_id=s.order_id and ol.sku=s.sku and ol.ba_site=s.ba_site
        group by order_number, order_id, o.expected_delivery_date, ol.ba_site
    ) orders
        on ifnull(ticket.custom_order_id, ticket.custom_order_number) = orders.order_number and orders.ba_site=left(ticket.id, 2)
    left outer join (
        select 
            string_agg(distinct carrier_code) as carrier_codes, 
            string_agg(distinct title) as title, 
            order_id 
        from {{ ref('stg__sales_flat_shipment_track') }}
        group by 3
    ) track
        on orders.order_id = track.order_id
	where 1=1
/*	{% if is_incremental() %}
		and updated_at >= '{{min_ts}}'
	{% endif %}*/
)

select * from tickets