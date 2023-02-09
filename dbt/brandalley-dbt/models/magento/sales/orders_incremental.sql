{{ config(
	materialized='incremental',
	unique_key='increment_id'
) }}

with order_updates as (
	select 
		* 
	from {{ ref('stg__sales_flat_order') }}
	where 1=1
	{% if is_incremental() %}
		and _streamkap_source_ts_ms > (select max(streamkap_updated_at) from {{this}})
	{% endif %}
),

order_sequencing as (
	-- determine sequencing using all orders the customer has ever had
	select
		increment_id,
		case 
			when status <> 'canceled' then row_number() over (partition by customer_id, status <> 'canceled' order by increment_id) 
			else null 
		end as orderno,
		case 
			when coalesce(total_paid,0) <> coalesce(total_refunded,0) and status <> 'canceled' 
			then row_number() over (partition by customer_id, coalesce(total_paid,0) <> coalesce(total_refunded,0) and status <> 'canceled' order by increment_id) 
			else null 
		end as order_number_excl_full_refunds,
		timestamp_diff(
			cast(created_at as timestamp), 
			lag(cast(created_at as timestamp)) over (partition by customer_id order by cast(created_at as timestamp))
		, day) as interval_between_orders
	from {{ ref('stg__sales_flat_order') }}
	where 1=1
	{% if is_incremental() %}
		and customer_id in (select distinct customer_id from order_updates)
	{% endif %}
),

order_info as (
	select
		sfo.increment_id,
		sfo.entity_id 														as magentoID,
		sfo.store_id,
		sfo.billing_address_id,
		sfo.shipping_address_id,
		sfo.subtotal_incl_tax,
		sfo.subtotal													    as subtotal_excl_tax,
		sfo.discount_amount total_discount_amount,
		sfo.base_free_shipping_amount 										as shipping_discount_amount,
		sfo.tax_amount 														as total_tax,
		sfo.shipping_amount 												as shipping_excl_tax,
		sfo.base_shipping_incl_tax 											as shipping_incl_tax,
		sfo.grand_total,
		sfo._streamkap_source_ts_ms 										as streamkap_updated_at,
		if(sfo.total_paid is null, 0, sfo.total_paid) 						as total_paid,
		coalesce(sfo.total_refunded, 0) 									as total_refunded,
		sfo.total_due,
		sfo.base_total_invoiced_cost 										as total_invoiced_cost,
		sfo.base_grand_total,
		sfo.status,
		sfo.coupon_rule_name,
		sfo.coupon_code,
		if(sfop.method = 'braintreevzero', sfop.cc_type, sfop.method) 		as method,
		sfo.shipping_method,
		sfo.shipping_description,
		sfo.customer_id,
		'' 																	as customer_name,
		'' 																	as customer_phone,
		'' 																	as delivery_address,
		sfoa.postcode 														as delivery_postcode,
		'' 																	as customer_age,
		sfo.expected_delivery_date,
		sfo.expected_delivery_days,
		cast(sfo.created_at as timestamp) 									as created_at,
		sfo.updated_at,
		sfo.total_qty_ordered,
		ce.email,
		sfo.customer_firstname,
		sfo.customer_lastname,
		cc_trans_id, 
		additional_information
	from order_updates sfo
	left join {{ ref('stg__sales_flat_order_address') }} sfoa
		on sfoa.entity_id = sfo.shipping_address_id
	left join {{ ref('stg__sales_flat_order_address') }} sfoa_b
		on sfoa_b.entity_id = sfo.billing_address_id
	left join {{ ref('stg__sales_flat_order_payment') }} sfop
		on sfo.entity_id = sfop.parent_id
	left join {{ ref('stg__customer_entity') }} ce
			on ce.entity_id = sfo.customer_id
	where 1=1
		and sfo.increment_id not like '%-%'
		and (sfo.sales_product_type != 12 or sfo.sales_product_type is null)
		{% if is_incremental() %}
		 	-- this isn't really doing anything to limit amount of table scan currently but have put in for future
			and sfoa.entity_id 		in (select shipping_address_id from order_updates)
			and sfoa_b.entity_id 	in (select billing_address_id from order_updates)
			and sfop.parent_id 		in (select entity_id from order_updates)
		{% endif %}
)

select 
	oi.*,
	sum(oi.interval_between_orders) over (partition by oi.customer_id order by oi.created_at) as total_interval_between_orders_for_each_customer,
	os.orderno,
	os.order_number_excl_full_refunds,
	os.interval_between_orders
from order_info oi
left join order_sequencing os
	on oi.increment_id = os.increment_id