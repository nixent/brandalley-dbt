{{ config(
	materialized='table',
	unique_key='id'
) }}


select  
    t.id,
    t.ba_site,
    t.created_at,
    t.description,
    t.due_at,
    t.is_public,
    t.merged_ticket_ids,
    t.status,
    t.subject,
    t.updated_at,
    t.type,
    t.url,
    t.via_channel,
    t.via_source_rel,
    if(t.via_channel='voice', 1, 0) as phone_ticket,
    if(t.via_channel='native_messaging', 1, 0) as chat_ticket,
    if(t.via_channel in ('web','email'), 1, 0) as web_email_ticket,
    if(t.via_channel='web', 1, 0) as web_ticket,
    if(t.via_channel='email', 1, 0) as email_ticket,
    t.custom_carrier,
    t.custom_client_contact_reason_no_order,
    t.custom_client_contact_reason_order,
    t.custom_contact_reason,
    t.custom_coupon_code,
    t.custom_needs_senior_help,
    t.custom_order_id,
    t.custom_order_number,
    t.custom_order_related,
    t.custom_product_issue_type,
    t.custom_shipping_issue_type,
    t.custom_solution,
    t.custom_status_id,
    t.custom_tracking_,
    ol.consignment_qty, 
    ol.warehouse_qty, 
    ol.selffulfill_qty,
    ol.order_id,
    tr.carrier_codes,
    tr.title
from {{ source('zendesk', 'ticket') }} t
left outer join (
    select order_number, order_id, ba_site, sum(consignment_qty) as consignment_qty, sum(warehouse_qty) as warehouse_qty, sum(selffulfill_qty) as selffulfill_qty
    from {{ ref('OrderLines') }} 
    group by 1,2,3
) ol
    on ifnull(t.custom_order_id, t.custom_order_number) = ol.order_number and t.ba_site = ol.ba_site
left outer join (
    select 
        string_agg(distinct carrier_code) as carrier_codes, 
        string_agg(distinct title) as title, 
        ba_site,
        order_id 
    from {{ ref('stg__sales_flat_shipment_track') }}
    group by 3,4 
) tr
    on ol.order_id = tr.order_id and ol.ba_site = tr.ba_site
/*	{% if is_incremental() %}
    and updated_at >= '{{min_ts}}'
{% endif %}*/
