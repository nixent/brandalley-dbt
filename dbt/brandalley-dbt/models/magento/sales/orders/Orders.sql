{{ config(
	materialized='incremental',
	unique_key=['increment_id', 'ba_site'],
	cluster_by=['status','customer_id'],
	partition_by = {
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	post_hook="delete from {{this}} where status in ('pending_payment', 'canceled')"
) }}

with order_updates as (
	select 
		* 
	from {{ ref('stg__sales_flat_order') }}
	where 1=1
		and increment_id not like '%-%'
		and (sales_product_type != 12 or sales_product_type is null)
	{% if is_incremental() %}
		and bq_last_processed_at >= (select max(bq_last_processed_at) from {{this}})
	{% endif %}
),

order_sequencing as (
	-- determine sequencing using all orders the customer has ever had
	select
		increment_id,
		ba_site,
		case 
			when status <> 'canceled' then row_number() over (partition by customer_id, ba_site, status <> 'canceled' order by timestamp(created_at)) 
			else null 
		end as orderno,
		case 
			when coalesce(total_paid,0) <> coalesce(total_refunded,0) and status not in ('canceled', 'closed')
			then row_number() over (partition by customer_id, ba_site, coalesce(total_paid,0) <> coalesce(total_refunded,0) and status not in ('canceled', 'closed') order by timestamp(created_at)) 
			else null 
		end as order_number_excl_full_refunds,
		row_number() over (partition by customer_id, ba_site order by timestamp(created_at)) as order_number_incl_cancellations,
		case 
			when status not in ('canceled')
			then timestamp_diff(
				timestamp(created_at), 
				lag(timestamp(created_at)) over (partition by customer_id, ba_site, status not in ('canceled') order by timestamp(created_at))
				, day)
			else null 
		end as interval_between_orders,
		timestamp_diff(
			timestamp(created_at), 
			first_value(timestamp(created_at)) over (partition by customer_id, ba_site order by timestamp(created_at))
		, day) as days_since_first_purchase
	from {{ ref('stg__sales_flat_order') }}
	where 1=1
	{% if is_incremental() %}
		and customer_id || '-' || ba_site in (select distinct customer_id || '-' || ba_site from order_updates)
	{% endif %}
),

order_info as (
	select
		sfo.increment_id,
		sfo.entity_id 														as order_id,
        sfo.ext_order_id                                                    as pe_order_id,
        sfo.exported_to_mageorder,
		sfo.store_id,
		sfo.ba_site,
		sfo.billing_address_id,
		sfo.shipping_address_id,
		sfo.subtotal_incl_tax,
		sfo.subtotal													    as subtotal_excl_tax,
		sfo.discount_amount 												as total_discount_amount,
		sfo.base_discount_amount 											as base_discount_amount,
		sfo.base_free_shipping_amount 										as shipping_discount_amount,
		sfo.tax_amount 														as total_tax,
		sfo.shipping_amount 												as shipping_excl_tax,
		sfo.base_shipping_incl_tax 											as shipping_incl_tax,
		sfo.grand_total,
		sfo.bq_last_processed_at,
		if(sfo.total_paid is null, 0, sfo.total_paid) 						as total_paid,
		coalesce(sfo.total_refunded, 0) 									as total_refunded,
		if(sfo.shipping_refunded is null, 0, sfo.shipping_refunded) 		as shipping_refunded,
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
		sfoa.postcode 														as delivery_postcode,
		sfoa_b.postcode														as billing_postcode,
		sfoa_b.address_type													as billing_address_type,
		sfo.expected_delivery_date,
		sfo.expected_delivery_days,
		cast(sfo.created_at as timestamp) 									as created_at,
		sfo.updated_at,
		sfo.total_qty_ordered,
		cc_trans_id, 
		additional_information,
		timestamp_diff(safe_cast(sfo.created_at as timestamp), safe_cast(ce.dt_cr as timestamp), day ) as days_since_signup,
        sfoa.country_id
	from order_updates sfo
	left join {{ ref('stg__sales_flat_order_address') }} sfoa
		on sfoa.entity_id = sfo.shipping_address_id
			and sfoa.ba_site = sfo.ba_site
	left join {{ ref('stg__sales_flat_order_address') }} sfoa_b
		on sfoa_b.entity_id = sfo.billing_address_id
			and sfoa_b.ba_site = sfo.ba_site
	left join {{ ref('stg__sales_flat_order_payment') }} sfop
		on sfo.entity_id = sfop.parent_id
			and sfop.ba_site = sfo.ba_site
	left join {{ ref('customers') }} ce
		on ce.cst_id = sfo.customer_id
			and ce.ba_site = sfo.ba_site
)

select 
	oi.ba_site || '-' || oi.increment_id as ba_site_increment_id,
	oi.*,
	-- check this below, whats it for?
	sum(os.interval_between_orders) over (partition by oi.customer_id order by oi.created_at) as total_interval_between_orders_for_each_customer,
	os.orderno,
	os.order_number_excl_full_refunds,
	os.order_number_incl_cancellations,
	os.interval_between_orders,
	os.days_since_first_purchase,
    ship.cnt as nb_shipment
from order_info oi
left join order_sequencing os
	on oi.increment_id = os.increment_id
		and oi.ba_site = os.ba_site
left join (
        select
            ba_site,
            order_id,
            count(*) as cnt
        from {{ ref('stg__sales_flat_shipment_track') }}
        group by 1, 2
    ) ship
        on oi.order_id=ship.order_id and os.ba_site=ship.ba_site