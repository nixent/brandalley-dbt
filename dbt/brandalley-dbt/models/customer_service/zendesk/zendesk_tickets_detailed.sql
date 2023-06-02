{{ config(
	materialized='table',
	unique_key='id'
) }}

with tickets as (
    select  id,
            created_at,
            description,
            due_at,
            is_public,
            merged_ticket_ids,
            status,
            subject,
            updated_at,
            type,
            url,
            via_channel,
            via_source_rel,
            if(via_channel='voice', 1, 0) as phone_ticket,
            if(via_channel='native_messaging', 1, 0) as chat_ticket,
            if(via_channel in ('web','email'), 1, 0) as web_email_ticket,
            if(via_channel='web', 1, 0) as web_ticket,
            if(via_channel='email', 1, 0) as email_ticket,
            custom_carrier,
            custom_client_contact_reason_no_order,
            custom_client_contact_reason_order,
            custom_contact_reason,
            custom_coupon_code,
            custom_needs_senior_help,
            custom_order_id,
            custom_order_number,
            custom_order_related,
            custom_product_issue_type,
            custom_shipping_issue_type,
            custom_solution,
            custom_status_id,
            custom_tracking_,
            consignment_qty, 
            warehouse_qty, 
            selffulfill_qty,
            order_id
    from {{ source(
        'zendesk',
        'ticket'
    ) }} ticket
    left outer join   (select sum(consignment_qty) as consignment_qty, sum(warehouse_qty) as warehouse_qty, sum(selffulfill_qty) as selffulfill_qty, order_number, order_id
    from {{ ref('OrderLines') }}
    group by order_number, order_id) orderlines
    on IFNULL(ticket.custom_order_id, ticket.custom_order_number) = orderlines.order_number
	where 1=1
/*	{% if is_incremental() %}
		and updated_at >= '{{min_ts}}'
	{% endif %}*/
)

select * from tickets