{{ config(
	materialized='incremental',
	unique_key='unique_id',
	cluster_by=['order_status','order_id'],
	partition_by = {
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	post_hook="delete from {{this}} where order_status in ('pending_payment', 'canceled')"
) }}

{% set min_ts = '2023-02-01' %}
{% if execute and is_incremental() %}
  {% set sql %}
    -- Query to see the earliest event date that needs to be rebuilt from for inserted order lines since last run  
    select min(created_at) as min_ts from (
		select 
			min(created_at) as created_at
		from {{ ref('Orders') }} 
		where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

		union all

		select 
			min(safe_cast(created_at as timestamp)) as created_at
		from {{ ref('stg__sales_flat_order_item') }} 
		where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

		union all

		select 
			min(safe_cast(fx_date_month as timestamp)) as created_at
		from {{ ref('fx_rates') }} 
		where updated_at >= ( select timestamp_sub(max(bq_last_processed_at),interval 30 minute) from {{this}} )
	)
  {% endset %}
  {% set result = run_query(sql) %}
  {% set min_ts = result.columns['min_ts'][0]  %}
{% endif %}

with order_lines as (
	select
		-- check unique key on this
		{{dbt_utils.generate_surrogate_key(['sfo.ba_site','sfoi_con.product_id','sfoi_con.order_id','sfoi_con.item_id','sfoi_sim.sku','eaov_size.option_id','cpei_size_child.entity_id','eaov_size_child.option_id'])}} as unique_id,
		greatest(sfo.bq_last_processed_at, sfoi_sim.bq_last_processed_at, sfoi_con.bq_last_processed_at)													as bq_last_processed_at,
		-- sometimes these are before reg date - how? should we set them as first_purchase_at in these cases?
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, month) 																				as months_since_cohort_start,
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, year) 																				as years_since_cohort_start,
		datetime_diff(safe_cast(sfo.created_at as datetime), ce.dt_cr, quarter) 																			as quarters_since_cohort_start,
		sfo.increment_id 																																	as order_number,
		sfo.customer_id,
		sfo.ba_site,
		sfoi_sim.item_id 																																	as order_item_id,
        sfoi_sim.parent_item_id,        
		sfo.magentoID 																																		as order_id,
		sfoi_sim.sku,
		if(au.user_id is not null, concat(au.firstname, ' ', au.lastname), 'Unknown') 																		as buyer,
		sfoi_con.name,
		sfoi_con.qty_canceled,
		sfoi_sim.qty_ordered,
		sfoi_sim.qty_invoiced,
		sfoi_con.qty_refunded,
		sfoi_con.qty_shipped,
        sfoi_sim.qty_ordered-sfoi_con.qty_canceled-sfoi_con.qty_refunded-sfoi_con.qty_shipped                                                               as qty_to_ship,
		if(sfoi_sim.qty_backordered is null or cpn.type=30, 0, sfoi_sim.qty_backordered) 																	as consignment_qty,
		if(sfoi_sim.qty_backordered is null or cpn.type!=30, 0, sfoi_sim.qty_backordered) 																	as selffulfill_qty,
		if(sfoi_sim.qty_backordered is null, sfoi_sim.qty_ordered, sfoi_sim.qty_ordered - sfoi_sim.qty_backordered) 										as warehouse_qty,
		safe_cast(sfo.created_at as datetime) 																												as order_placed_date,
		sfoi_con.dispatch_date 																																as dispatch_due_date,
		cast((sfoi_sim.base_cost) as decimal) 																												as product_cost_exc_vat,
		(sfoi_sim.base_cost) * sfoi_sim.qty_ordered 																										as line_product_cost_exc_vat,
		sfoi_con.original_price 																															as flash_price_inc_vat,
		sfoi_con.original_price * sfoi_sim.qty_ordered 																										as line_flash_price_inc_vat,
		sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) 																				as flash_price_exc_vat,
		sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) * sfoi_sim.qty_ordered 														as line_flash_price_exc_vat,
		sfoi_con.discount_amount 																															as line_discount_amount,
		if(sfo.total_refunded is null, 0, sfo.total_refunded)																								as line_total_refunded,
		safe_divide(sfo.shipping_incl_tax,sfo.total_qty_ordered) * sfoi_sim.qty_ordered as line_shipping_incl_tax,
		safe_divide(sfo.shipping_excl_tax,sfo.total_qty_ordered) * sfoi_sim.qty_ordered as line_shipping_excl_tax,
		shipping_refunded,
		if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value) 														as category_path,
		case 
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Home>%') then "Homeware"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Home & Garden>%') then "Homeware"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Gifts>%') then "Festival"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Kids>%') then "Kidswear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Christmas>%') then "Festival"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Lingerie>%') then "Lingerie & Swimwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Outlet>%') then "Outlet"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Tops%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Trousers>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Underwear>%') then "Lingerie & Swimwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Accessories>%') then "Accessories"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Clothing>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Fleeces%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Jeans>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Knitwear>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Onesies>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Outerwear>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Shirts>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Footwear>%') then "Footwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Shoes>%') then "Footwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Shorts>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Sportswear%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Suits>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Swimwear>%') then "Lingerie & Swimwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Men>Nightwear>%') then "Lingerie & Swimwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Sports%') then "Active"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Accessories%') then "Accessories"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Blouses%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Clothing>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Dresses>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Fleeces%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Footwear%') then "Footwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Handbags>%') then "Accessories"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Jeans>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Knitwear>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Lingerie>%') then "Lingerie & Swimwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Onesies>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Outerwear>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Playsuits%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Shoes%') then "Footwear"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Shorts>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Skirts>%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Sportswear%') then "Active"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Tops%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Trousers%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Maternity%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Jeans%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Women>Outerwear%') then "RTW"
            when if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value)
            like ('Beauty>%') then "Beauty"
            else "Others" end 														                                                                        as marketing_category,
		coalesce(eaov_pt_con.value, eaov_pt_sim.value, 'Unknown') 																							as product_type,
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
			when lower(ccfse.path_name) like '%>clearance>%' then 'CLEARANCE'
			when lower(cceh.name) = 'outlet' then 'OUTLET'
			when eaov_brand.value = 'N°· Eleven' then 'OWN BRAND'
			when cceh.name is not null then ptd.product_department
			else 'OUTLET'
		end 																																				as department_type,
		sfo.updated_at,
		safe_cast(sfo.created_at as timestamp) 																												as created_at,
		cast(null as datetime) 																																as month_created,
		sfo.status 																																			as order_status,
		sfoi_con.tax_amount,
		sfoi_con.tax_percent,
		cped_price.value 																																	as rrp,
		ce.dt_cr 																																			as reg_date,
		{{ calculate_region_from_postcode('sfo.billing_postcode') }} 																						as region,
		{{ calculate_region_from_postcode('sfo.delivery_postcode') }} 																						as shipping_region,
		sfo.billing_address_type, -- Cat 1  
		safe_cast(cpn.date_comp_exported as timestamp) 																										as date_comp_exported,
		sfoi_sim.created_at > cpn.date_comp_exported 																										as cpn_date_flag,
		sfoi_sim.qty_backordered,
		cpn.sap_ref,
		cpn.status 																																			as cpn_status,
		eaov_product_age.value																																as product_age,
        row_number() over (partition by sfo.increment_id order by sfoi_con.dispatch_date, sfoi_sim.sku asc)                                                 as shipping_order,
		coalesce(cpe.sku, 'Unknown') 																														as parent_sku,
		cpr.reference																																		as REFERENCE,
		(sfoi_sim.qty_ordered * sfoi_con.base_price_incl_tax) - sfoi_con.base_discount_amount 																as total_local_currency_after_vouchers,
		sfoi_sim.qty_ordered * sfoi_con.base_price_incl_tax 																								as total_local_currency_before_vouchers,
		sfoi_sim.qty_ordered * sfoi_con.base_price - (sfoi_con.base_discount_amount - IFNULL(sfoi_con.hidden_tax_amount,0))			                		as total_local_currency_ex_tax_after_vouchers,
		sfoi_sim.qty_ordered * sfoi_con.base_price											                                                        		as total_local_currency_ex_tax_before_vouchers
	from {{ ref('Orders') }} sfo
	left join {{ ref('customers') }} ce 
		on ce.cst_id = sfo.customer_id and ce.ba_site = sfo.ba_site
	left join {{ ref('stg__sales_flat_order_item') }} sfoi_sim
		on sfoi_sim.order_id = sfo.magentoID
			and sfoi_sim.product_type = 'simple'
			and sfo.ba_site = sfoi_sim.ba_site
	left join {{ ref('stg__sales_flat_order_item') }} sfoi_con
		on sfoi_con.order_id = sfo.magentoID
			and if (sfoi_sim.parent_item_id is not null, sfoi_con.item_id = sfoi_sim.parent_item_id, sfoi_con.item_id = sfoi_sim.item_id)
			and sfo.ba_site = sfoi_con.ba_site
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_con
		on cpev_outletcat_con.entity_id = sfoi_con.product_id
			and cpev_outletcat_con.attribute_id = 205
			and cpev_outletcat_con.store_id = 0
			and sfoi_con.ba_site = cpev_outletcat_con.ba_site
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_sim
		on cpev_outletcat_sim.entity_id = sfoi_sim.product_id
			and cpev_outletcat_sim.attribute_id = 205
			and cpev_outletcat_sim.store_id = 0
			and sfoi_sim.ba_site = cpev_outletcat_sim.ba_site
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_sim
		on cpev_pt_sim.entity_id = sfoi_sim.product_id
			and cpev_pt_sim.attribute_id = 179
			and cpev_pt_sim.store_id = 0
			and sfoi_sim.ba_site = cpev_pt_sim.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt_sim
		on cpev_pt_sim.value = cast(eaov_pt_sim.option_id as string)
			and eaov_pt_sim.store_id = 0
			and cpev_pt_sim.ba_site = eaov_pt_sim.ba_site
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_con
		on cpev_pt_con.entity_id = sfoi_con.product_id
			and cpev_pt_con.attribute_id = 179
			and cpev_pt_con.store_id = 0
			and sfoi_con.ba_site = cpev_pt_con.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt_con
		on cpev_pt_con.value = cast(eaov_pt_con.option_id as string)
			and eaov_pt_con.store_id = 0
			and cpev_pt_con.ba_site = eaov_pt_con.ba_site
	left join {{ ref('product_type_department') }} ptd
		on lower(coalesce(eaov_pt_con.value,eaov_pt_sim.value)) = lower(ptd.product_type)
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_brand
		on cpei_brand.entity_id = sfoi_con.product_id
			and cpei_brand.attribute_id = 178
			and cpei_brand.store_id = 0
			and sfoi_con.ba_site = cpei_brand.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_brand
		on eaov_brand.option_id = cpei_brand.value
			and eaov_brand.store_id = 0
			and cpei_brand.ba_site = eaov_brand.ba_site
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_color
		on cpei_color.entity_id = sfoi_con.product_id
			and cpei_color.attribute_id = 213
			and cpei_color.store_id = 0
			and cpei_color.ba_site = sfoi_con.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_color
		on eaov_color.option_id = cpei_color.value
			and eaov_color.store_id = 0
			and eaov_color.ba_site = cpei_color.ba_site
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_product_age
		on cpei_product_age.entity_id = sfoi_con.product_id
			and cpei_product_age.attribute_id = 213
			and cpei_product_age.store_id = 0
			and cpei_product_age.ba_site = sfoi_con.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_product_age
		on eaov_product_age.option_id = cpei_product_age.value
			and eaov_product_age.store_id = 0
			and eaov_product_age.ba_site = cpei_product_age.ba_site
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_size
		on cpei_size.entity_id = sfoi_con.product_id
			and cpei_size.attribute_id = 177
			and cpei_size.store_id = 0
			and cpei_size.ba_site = sfoi_con.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_size
		on eaov_size.option_id = cpei_size.value
			and eaov_size.store_id = 0
			and eaov_size.ba_site = cpei_size.ba_site
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_size_child
		on cpei_size_child.entity_id = sfoi_sim.product_id
			and cpei_size_child.attribute_id = 177
			and cpei_size_child.store_id = 0
			and cpei_size_child.ba_site = sfoi_sim.ba_site
	left join {{ ref('stg__eav_attribute_option_value') }} eaov_size_child
		on eaov_size_child.option_id = cpei_size_child.value
			and eaov_size_child.store_id = 0
			and eaov_size_child.ba_site = cpei_size_child.ba_site
	left join {{ ref('stg__catalog_product_entity_int') }} cpei_supplier
		on cpei_supplier.entity_id = sfoi_sim.product_id
			and cpei_supplier.attribute_id = 239
			and cpei_supplier.store_id = 0
			and cpei_supplier.ba_site = sfoi_sim.ba_site
	left join {{ ref('stg__catalog_product_supplier') }} cps_supplier
		on cpei_supplier.value = cps_supplier.supplier_id
		and cpei_supplier.ba_site = cps_supplier.ba_site
	left join {{ ref('stg__sales_flat_order_item_extra') }} sfoie
		on sfoi_con.item_id = sfoie.order_item_id
		and sfoi_con.ba_site = sfoie.ba_site
	left join {{ ref('stg__catalog_category_entity_history') }} cceh
		on sfoie.category_id = cceh.category_id
		and sfoie.ba_site = cceh.ba_site
	left join {{ ref('catalog_category_flat_store_1_enriched') }} ccfse
		on ccfse.entity_id = cceh.category_id
		and ccfse.ba_site = cceh.ba_site
	left join {{ ref('stg__catalog_product_negotiation') }} cpn
		on sfoi_sim.nego = cpn.negotiation_id
		and cpn.ba_site = sfoi_sim.ba_site
	left join {{ ref('stg__admin_user') }} au
		on cpn.buyer = au.user_id
		and cpn.ba_site = au.ba_site
	-- todo split this into a sep dedupe model
	left join (
		select * from {{ ref('stg__catalog_product_super_link') }}
		qualify row_number() over (partition by product_id, ba_site order by link_id desc) = 1
	) cpsl
		on sfoi_sim.product_id = cpsl.product_id and cpsl.ba_site = sfoi_sim.ba_site
	left outer join {{ ref('stg__catalog_product_entity') }} cpe
		on cpe.entity_id = cpsl.parent_id
		and cpe.ba_site = cpsl.ba_site
	left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_gender 
		on cpe.entity_id = cpev_gender.entity_id
			and cpev_gender.attribute_id = 180
			and cpev_gender.store_id = 0
			and cpe.ba_site = cpev_gender.ba_site
	left join {{ ref('stg__catalog_product_entity_decimal') }} cped_price
		on sfoi_sim.product_id = cped_price.entity_id
			and cped_price.attribute_id = 75
			and cped_price.store_id = 0
			and cped_price.ba_site = sfoi_sim.ba_site
	left join {{ ref('stg__catalog_product_entity') }} cpe_ref
		on sfoi_sim.sku = cpe_ref.sku
		and cpe_ref.ba_site = sfoi_sim.ba_site
	left join (
			select entity_id, ba_site, reference from {{ ref('stg__catalog_product_reference') }}
			qualify row_number() over (partition by entity_id, ba_site order by reference_id desc) = 1
		) cpr
		on cpe_ref.entity_id = cpr.entity_id
		and cpe_ref.ba_site = cpr.ba_site

	where 1=1
	{% if is_incremental() %}
		and sfo.created_at >= '{{min_ts}}'
	{% endif %}

)


select 
	ol.*,
	ol.total_local_currency_ex_tax_after_vouchers - ol.line_product_cost_exc_vat 																	as margin,
	initcap(split(ol.category_path, '>')[safe_offset(0)]) 																							as product_category_level_1, 
	initcap(split(ol.category_path, '>')[safe_offset(1)]) 																							as product_category_level_2,
	initcap(split(ol.category_path, '>')[safe_offset(2)]) 																							as product_category_level_3,
	row_number() over (partition by ol.order_number, ol.parent_sku, ol.ba_site order by ol.sku) 													as parent_sku_offset,
	if(ol.ba_site = 'FR', round(ol.total_local_currency_after_vouchers * fx.eur_to_gbp,2), ol.total_local_currency_after_vouchers) 					as TOTAL_GBP_after_vouchers,
	if(ol.ba_site = 'FR', round(ol.total_local_currency_before_vouchers * fx.eur_to_gbp,2), ol.total_local_currency_before_vouchers)				as TOTAL_GBP_before_vouchers,
	if(ol.ba_site = 'FR', round(ol.total_local_currency_ex_tax_after_vouchers * fx.eur_to_gbp,2), ol.total_local_currency_ex_tax_after_vouchers)	as TOTAL_GBP_ex_tax_after_vouchers,
	if(ol.ba_site = 'FR', round(ol.total_local_currency_ex_tax_before_vouchers * fx.eur_to_gbp,2), ol.total_local_currency_ex_tax_before_vouchers)	as TOTAL_GBP_ex_tax_before_vouchers,
	line_shipping_incl_tax - line_shipping_excl_tax																									as line_shipping_tax
from order_lines ol
left join {{ ref('fx_rates') }} fx
	on date(ol.created_at) = fx.date_day

where ol.unique_id != 'f66682c5a4027e12ec8c24f2a24628ea'
