{{ config(
	materialized='incremental',
	unique_key='unique_id',
	cluster_by='order_id',
	partition_by = {
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

{% set min_ts = '2023-02-01' %}
{% if execute and is_incremental() %}
  {% set sql %}
    -- Query to see the earliest event date that needs to be rebuilt from for inserted order lines since last run  
    select min(created_at) as min_ts from (
		select 
			min(created_at) as created_at
		from {{ ref('orders_incremental') }} 
		where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

		union all

		select 
			min(safe_cast(created_at as timestamp)) as created_at
		from {{ ref('stg__sales_flat_order_item') }} 
		where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )
	)
  {% endset %}
  {% set result = run_query(sql) %}
  {% set min_ts = result.columns['min_ts'][0]  %}
{% endif %}

with order_lines as (
	select
		{{dbt_utils.surrogate_key(['sfoi_con.product_id','sfoi_con.order_id','sfoi_con.item_id','sfo.magentoID','cpev_pt_con.value','eaov_brand.option_id','eaov_color.option_id','eaov_size.option_id','cpei_size_child.entity_id','eaov_size_child.option_id'])}} as unique_id,
		greatest(sfo.bq_last_processed_at, sfoi_sim.bq_last_processed_at, sfoi_con.bq_last_processed_at)													as bq_last_processed_at,
		-- sometimes these are before reg date - how? should we set them as first_purchase_at in these cases?
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, month) 																				as months_since_cohort_start,
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, year) 																				as years_since_cohort_start,
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, quarter) 																			as quarters_since_cohort_start,
		sfo.increment_id 																																	as order_number,
		sfo.customer_id 																																	as customer_id,
		sfoi_sim.item_id 																																	as order_item_id,
		sfo.magentoID 																																		as order_id,
		sfoi_sim.sku,
		if(au.user_id is not null, concat(au.firstname, ' ', au.lastname), 'Unknown') 																		as buyer,
		sfoi_con.name,
		sfoi_con.qty_canceled,
		sfoi_sim.qty_ordered,
		sfoi_sim.qty_invoiced,
		sfoi_con.qty_refunded,
		sfoi_con.qty_shipped,
		if(sfoi_sim.qty_backordered is null or cpn.type=30, 0, sfoi_sim.qty_backordered) 																	as consignment_qty,
		if(sfoi_sim.qty_backordered is null or cpn.type!=30, 0, sfoi_sim.qty_backordered) 																	as selffulfill_qty,
		if(sfoi_sim.qty_backordered is null, sfoi_sim.qty_ordered, sfoi_sim.qty_ordered - sfoi_sim.qty_backordered) 										as warehouse_qty,
		safe_cast(sfo.created_at as datetime) 																												as order_placed_date,
		case
			when sfoi_con.dispatch_date < cast('2014-06-11' as date) then null
			else sfoi_con.dispatch_date
		end 																																				as dispatch_due_date,
		sfoi_sim.base_cost 																																	as product_cost_inc_vat,
		(sfoi_sim.base_cost * sfoi_sim.qty_ordered) 																										as line_product_cost_inc_vat,
		cast((sfoi_sim.base_cost) as decimal) 																												as product_cost_exc_vat,
		(sfoi_sim.base_cost) * sfoi_sim.qty_ordered 																										as line_product_cost_exc_vat,
		sfoi_con.original_price 																															as flash_price_inc_vat,
		sfoi_con.original_price * sfoi_sim.qty_ordered 																										as line_flash_price_inc_vat,
		sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) 																				as flash_price_exc_vat,
		sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) * sfoi_sim.qty_ordered 														as line_flash_price_exc_vat,
		sfoi_con.discount_amount 																															as line_discount_amount,
		cast((
			((sfoi_con.original_price * sfoi_sim.qty_ordered)/nullif((1 +(sfoi_con.tax_percent / 100.)),0)) - (sfoi_sim.base_cost * sfoi_sim.qty_ordered))
			/nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered)/(1 +(sfoi_con.tax_percent / 100.))),0
		) as decimal) 																																		as margin_inc_discount_percentage,
		cast((
			(sfoi_con.original_price * sfoi_sim.qty_ordered)/nullif((1 +(sfoi_con.tax_percent / 100.)),0) - (sfoi_sim.base_cost * sfoi_sim.qty_ordered)
		) as decimal) 																																		as margin_inc_discount_value,
		cast((
			(((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount)/nullif((1 +(sfoi_con.tax_percent / 100.)),0)) 
			- (sfoi_sim.base_cost * sfoi_sim.qty_ordered))
			/nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 + (sfoi_con.tax_percent / 100.)),0)),0
		) as decimal) 																																		as margin_exc_discount_percentage,
		cast(
			(((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount)
			/nullif((1 +(sfoi_con.tax_percent / 100.)),0)) 
			- (sfoi_sim.base_cost * sfoi_sim.qty_ordered) 
		as decimal) 																																		as margin_exc_discount_value,
		if(sfo.total_refunded is null, 0, sfo.total_refunded)																								as line_total_refunded,
		shipping_refunded,
		if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value) 														as category_path,
		if(eaov_pt_con.value is not null, eaov_pt_con.value, eaov_pt_sim.value) 																			as product_type,
		eaov_brand.value 																																	as brand,
		cps_supplier.sup_id 																																as supplier_id,
		cps_supplier.name 																																	as supplier_name,
		eaov_color.value 																																	as colour,
		replace(replace(replace(cpev_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') 														as gender,
		ifnull(eaov_size.value, eaov_size_child.value) 																										as SIZE,
		sfoi_con.nego,
		case
			when cceh.name in ('', 'Women', 'Men', 'Kids', 'Lingerie', 'Home', 'Beauty', 'Z_NoData', 'Archieved outlet products', 'Holding review')
				or cceh.name is null
			then 'Outlet'
			else cceh.name
		end 																																				as category_name,
		case
			when cceh.type = 1 then coalesce(ptd.product_department, 'OUTLET')
			when cceh.type = 2 then 'CLEARANCE'
			when cceh.type = 3 then 'OUTLET'
			when cceh.type is null then 'OTHERS'
			else 'OUTLET'
		end 																																				as department_type,
		sfo.updated_at,
		cast(sfo.created_at as timestamp) 																													as created_at,
		cast(null as datetime) 																																as month_created,
		sfo.status 																																			as order_status,
		sfoi_con.tax_amount,
		sfoi_con.tax_percent,
		cped_price.value 																																	as rrp,
		ce.dt_cr 																																			as reg_date,
		{{ calculate_region_from_postcode('sfo.billing_postcode') }} 																						as Region, -- Cat 1
		sfo.email 																																			as customer_email, -- Cat 1
		sfo.billing_address_type, -- Cat 1  
		CAST(cpn.date_comp_exported as timestamp) 																											as date_comp_exported,
		sfoi_sim.created_at > cpn.date_comp_exported 																										as cpn_date_flag,
		sfoi_sim.qty_backordered,
		cpn.sap_ref,
		cpn.status 																																			as cpn_status,
		eaov_product_age.value																																as product_age,
		max(cpe.sku) 																																		as parent_sku,
		max(cpr.reference) 																																	as REFERENCE,
		sum((sfoi_sim.qty_ordered * sfoi_con.original_price) - sfoi_con.discount_amount) 																	as TOTAL_GBP_after_vouchers,
		sum(sfoi_sim.qty_ordered * sfoi_con.original_price) 																								as TOTAL_GBP_before_vouchers,
		sum(sfoi_sim.qty_ordered * (sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0)) - sfoi_con.discount_amount) 					as TOTAL_GBP_ex_tax_after_vouchers,
		sum(sfoi_sim.qty_ordered * (sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0))) 												as TOTAL_GBP_ex_tax_before_vouchers,
		sum(if(sfoi_sim.qty_backordered is null or cpn.type!=30, 0, sfoi_sim.qty_backordered) * sfoi_con.base_price_incl_tax) 								as selffulfill_totalGBP_inc_tax,
		sum(if(sfoi_sim.qty_backordered is null or cpn.type!=30, 0, sfoi_sim.qty_backordered) * sfoi_con.base_price) 										as selffulfill_totalGBP_ex_tax,       
		sum(if(sfoi_sim.qty_backordered is null or cpn.type=30, 0, sfoi_sim.qty_backordered) * sfoi_con.base_price_incl_tax) 								as consignment_totalGBP_inc_tax,
		sum(if(sfoi_sim.qty_backordered is null or cpn.type=30, 0, sfoi_sim.qty_backordered) * sfoi_con.base_price) 										as consignment_totalGBP_ex_tax,       
		sum(if(sfoi_sim.qty_backordered is null, sfoi_sim.qty_ordered, sfoi_sim.qty_ordered - sfoi_sim.qty_backordered) * sfoi_con.base_price_incl_tax) 	as warehouse_totalGBP_inc_tax,
		sum(if(sfoi_sim.qty_backordered is null, sfoi_sim.qty_ordered, sfoi_sim.qty_ordered - sfoi_sim.qty_backordered) * sfoi_con.base_price) 				as warehouse_totalGBP_ex_tax
	from {{ ref('orders_incremental') }} sfo
	left join {{ ref('customers_incremental') }} ce 
		on ce.cst_id = sfo.customer_id
	left join {{ ref('stg__sales_flat_order_item') }} sfoi_sim
		on sfoi_sim.order_id = sfo.magentoID
			and sfoi_sim.product_type = 'simple'
	left join {{ ref('stg__sales_flat_order_item') }} sfoi_con
		on sfoi_con.order_id = sfo.magentoID
			and if (sfoi_sim.parent_item_id is not null, sfoi_con.item_id = sfoi_sim.parent_item_id, sfoi_con.item_id = sfoi_sim.item_id)
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_con
		on cpev_outletcat_con.entity_id = sfoi_con.product_id
			and cpev_outletcat_con.attribute_id = 205
			and cpev_outletcat_con.store_id = 0
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_sim
		on cpev_outletcat_sim.entity_id = sfoi_sim.product_id
			and cpev_outletcat_sim.attribute_id = 205
			and cpev_outletcat_sim.store_id = 0
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_sim
		on cpev_pt_sim.entity_id = sfoi_sim.product_id
			and cpev_pt_sim.attribute_id = 179
			and cpev_pt_sim.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt_sim
		on cpev_pt_sim.value = cast(eaov_pt_sim.option_id as string)
			and eaov_pt_sim.store_id = 0
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_con
		on cpev_pt_con.entity_id = sfoi_con.product_id
			and cpev_pt_con.attribute_id = 179
			and cpev_pt_con.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt_con
		on cpev_pt_con.value = cast(eaov_pt_con.option_id as string)
			and eaov_pt_con.store_id = 0
	left join {{ ref('product_type_department') }} ptd
		on lower(coalesce(eaov_pt_con.value,eaov_pt_sim.value)) = ptd.product_type
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_brand
		on cpei_brand.entity_id = sfoi_con.product_id
			and cpei_brand.attribute_id = 178
			and cpei_brand.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_brand
		on eaov_brand.option_id = cpei_brand.value
			and eaov_brand.store_id = 0
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_color
		on cpei_color.entity_id = sfoi_con.product_id
			and cpei_color.attribute_id = 213
			and cpei_color.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_color
		on eaov_color.option_id = cpei_color.value
			and eaov_color.store_id = 0
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_product_age
		on cpei_product_age.entity_id = sfoi_con.product_id
			and cpei_product_age.attribute_id = 213
			and cpei_product_age.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_product_age
		on eaov_product_age.option_id = cpei_product_age.value
			and eaov_product_age.store_id = 0
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_size
		on cpei_size.entity_id = sfoi_con.product_id
			and cpei_size.attribute_id = 177
			and cpei_size.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_size
		on eaov_size.option_id = cpei_size.value
			and eaov_size.store_id = 0
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_size_child
		on cpei_size_child.entity_id = sfoi_sim.product_id
			and cpei_size_child.attribute_id = 177
			and cpei_size_child.store_id = 0
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_size_child
		on eaov_size_child.option_id = cpei_size_child.value
			and eaov_size_child.store_id = 0
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_supplier
		on cpei_supplier.entity_id = sfoi_sim.product_id
			and cpei_supplier.attribute_id = 239
			and cpei_supplier.store_id = 0
	left join {{ ref('stg__catalog_product_supplier') }} cps_supplier
		on cpei_supplier.value = cps_supplier.supplier_id
	left join {{ ref('stg__sales_flat_order_item_extra') }} sfoie
		on sfoi_con.item_id = sfoie.order_item_id
	left join {{ ref('stg__catalog_category_entity_history') }} cceh
		on sfoie.category_id = cceh.category_id
	left join {{ ref('stg__catalog_product_negotiation') }} cpn
		on sfoi_sim.nego = cpn.negotiation_id
	left join {{ ref('stg__admin_user') }} au
		on cpn.buyer = au.user_id
	-- todo split this into a sep dedupe model
	left join (
		select * from {{ ref('stg__catalog_product_super_link') }}
		qualify row_number() over (partition by product_id order by link_id desc) = 1
	) cpsl
		on sfoi_sim.product_id = cpsl.product_id
	left outer join {{ ref('stg__catalog_product_entity') }} cpe
		on cpe.entity_id = cpsl.parent_id
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_gender 
		on cpe.entity_id = cpev_gender.entity_id
			and cpev_gender.attribute_id = 180
			and cpev_gender.store_id = 0
	left join {{ ref('stg__catalog_product_entity_decimal') }} cped_price
		on sfoi_sim.product_id = cped_price.entity_id
			and cped_price.attribute_id = 75
			and cped_price.store_id = 0
	left join {{ ref('stg__catalog_product_entity') }} cpe_ref
		on sfoi_sim.sku = cpe_ref.sku
	left join {{ ref('stg__catalog_product_reference') }} cpr
		on cpe_ref.entity_id = cpr.entity_id

	where 1=1
	{% if is_incremental() %}
		and sfo.created_at > '{{min_ts}}'
	{% endif %}

	{{dbt_utils.group_by(65)}}
)


select 
	*,
	initcap(split(category_path, '>')[safe_offset(0)]) as product_category_level_1, 
	initcap(split(category_path, '>')[safe_offset(1)]) as product_category_level_2,
	initcap(split(category_path, '>')[safe_offset(2)]) as product_category_level_3
from order_lines

