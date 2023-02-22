{{ config(
	materialized='incremental',
	unique_key='cst_id'
) }}

{% if is_incremental() %}
with customers_updated as (
	select 
		entity_id as customer_id
	from {{ ref('stg__customer_entity') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_entity_int') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_entity_datetime') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_entity_varchar') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_entity_text') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_address_entity_text') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id
	from {{ ref('stg__customer_address_entity_varchar') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

	union all

	select 
		customer_id
	from {{ ref('stg__newsletter_subscriber') }} 
	where bq_last_processed_at >= ( select max(bq_last_processed_at) from {{this}} )

)
{% endif %}

select
	ce.entity_id 													   as cst_id,
	'' 																   as customer_name,
	'' 																   as email,
	'' 																   as telephone,
	'' 																   as billing_street,
	ca_b_26.value billing_city,
	ca_b_30.value billing_postcode,
	ca_b_28.value b_region,
	ca_b_27.value billing_country,
	'' 																   as shipping_street,
	ca_s_26.value shipping_city,
	ca_s_30.value shipping_postcode,
	ca_s_28.value s_region,
	ca_s_27.value s_country,
	safe_cast(safe_cast(ce.created_at as timestamp) as datetime) 	   as dt_cr,
	if(ns.subscriber_status = 1, 'Opted', 'Not Opted') 				   as subscription,
	if(cet_old_acount.value = '', null, cet_old_acount.value) 		   as old_account_id,
	if(cei_222.value = 1, 'Yes', 'No') 								   as third_party,
	ce.updated_at,
	cei_363.value 													   as achica_user,
	if(cei_367.value is null, timestamp(ce.created_at), cei_367.value) as achica_migration_date,
	ce.bq_last_processed_at
from {{ ref('stg__customer_entity') }} ce
left join {{ ref('stg__customer_entity_int') }} cei
	on ce.entity_id = cei.entity_id
		and cei.attribute_id = 13
left join {{ ref('stg__customer_entity_varchar') }} cev_5
	on ce.entity_id = cev_5.entity_id
		and cev_5.attribute_id = 5
left join {{ ref('stg__customer_entity_varchar') }} cev_7
	on ce.entity_id = cev_7.entity_id
       	and cev_7.attribute_id = 7
left join {{ ref('stg__customer_entity_text') }} cet_old_acount
	on ce.entity_id = cet_old_acount.entity_id
       	and cet_old_acount.attribute_id = 217
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_20
	on cei.value = ca_b_20.entity_id
       	and ca_b_20.attribute_id = 20
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_22
	on cei.value = ca_b_22.entity_id
       	and ca_b_22.attribute_id = 22
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_24
	on cei.value = ca_b_24.entity_id
		and ca_b_24.attribute_id = 24
left join {{ ref('stg__customer_address_entity_text') }} ca_b_25
	on cei.value = ca_b_25.entity_id
       	and ca_b_25.attribute_id = 25
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_26
	on cei.value = ca_b_26.entity_id
		and ca_b_26.attribute_id = 26
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_27
	on cei.value = ca_b_27.entity_id
       	and ca_b_27.attribute_id = 27
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_28
	on cei.value = ca_b_28.entity_id
       	and ca_b_28.attribute_id = 28
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_30
	on cei.value = ca_b_30.entity_id
       	and ca_b_30.attribute_id = 30
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_31
	on cei.value = ca_b_31.entity_id
       	and ca_b_31.attribute_id = 31
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_32
	on cei.value = ca_b_32.entity_id
       	and ca_b_32.attribute_id = 32
left join {{ ref('stg__customer_entity_int') }} cei_s
	on ce.entity_id = cei_s.entity_id
       	and cei_s.attribute_id = 14
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_20
	on cei_s.value = ca_s_20.entity_id
       	and ca_s_20.attribute_id = 20
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_22
	on cei_s.value = ca_s_22.entity_id
       	and ca_s_22.attribute_id = 22
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_24
	on cei_s.value = ca_s_24.entity_id
       	and ca_s_24.attribute_id = 24
left join {{ ref('stg__customer_address_entity_text') }} ca_s_25
	on cei_s.value = ca_s_25.entity_id
       	and ca_s_25.attribute_id = 25
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_26
	on cei_s.value = ca_s_26.entity_id
       	and ca_s_26.attribute_id = 26
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_27
	on cei_s.value = ca_s_27.entity_id
       	and ca_s_27.attribute_id = 27
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_28
	on cei_s.value = ca_s_28.entity_id
       	and ca_s_28.attribute_id = 28
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_30
	on cei_s.value = ca_s_30.entity_id
       	and ca_s_30.attribute_id = 30
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_31
	on cei_s.value = ca_s_31.entity_id
       	and ca_s_31.attribute_id = 31
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_32
	on cei_s.value = ca_s_32.entity_id
       	and ca_s_32.attribute_id = 32
left join (select distinct customer_id, subscriber_status from {{ ref('stg__newsletter_subscriber') }}) ns
	on ce.entity_id = ns.customer_id
left join {{ ref('stg__customer_entity_int') }}	cei_222
	on ce.entity_id = cei_222.entity_id
       	and cei_222.attribute_id = 222
left join {{ ref('stg__customer_entity_int') }} cei_363
	on cei_363.entity_id = ce.entity_id
       	and cei_363.attribute_id = 363
       	and (cei_363.value = 1 or cei_363.value = 2)
left join {{ ref('stg__customer_entity_datetime') }} cei_367
	on cei_367.entity_id = ce.entity_id
		and cei_367.attribute_id = 367
where 1=1
{% if is_incremental() %}
	and ce.entity_id in (select distinct customer_id from customers_updated)
{% endif %}
