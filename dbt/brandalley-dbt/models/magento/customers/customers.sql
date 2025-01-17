{{ config(
	materialized='incremental',
	unique_key='ba_site_customer_id'
) }}

{% if is_incremental() %}
with customers_updated as (
	select 
		entity_id as customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__customer_entity') }} 
	where bq_last_processed_at > ( select max(customer_bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__customer_entity_int') }} 
	where bq_last_processed_at > ( select max(customer_bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__customer_entity_datetime') }} 
	where bq_last_processed_at > ( select max(customer_bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__customer_entity_text') }} 
	where bq_last_processed_at > ( select max(customer_bq_last_processed_at) from {{this}} )

	union all

	select 
		entity_id as customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__customer_address_entity_varchar') }} 
	where bq_last_processed_at > ( select max(address_bq_last_processed_at) from {{this}} )

	union all

	select 
		customer_id, ba_site, bq_last_processed_at
	from {{ ref('stg__newsletter_subscriber') }} 
	where bq_last_processed_at > ( select max(subscriber_bq_last_processed_at) from {{this}} )
),

customers as
{% else %}
with customers as
{% endif %}
(

select
	ce.ba_site || '-' || ce.entity_id 								   as ba_site_customer_id,
	ce.entity_id 													   as cst_id,
	ce.email_hash,
	ce.ba_site,
	ca_b_26.value billing_city,
	ca_b_30.value billing_postcode,
	ca_b_28.value b_region,
	ca_b_27.value billing_country,
	ca_s_26.value shipping_city,
	ca_s_30.value shipping_postcode,
	ca_s_28.value s_region,
	ca_s_27.value s_country,
	safe_cast(safe_cast(ce.created_at as timestamp) as datetime) 	   as dt_cr,
	if(ns.subscriber_status = 1, 'Opted', 'Not Opted') 				   as subscription,
	if(cet_old_account.value = '', null, cet_old_account.value) 	   as old_account_id,
	if(cei_222.value = 1, 'Yes', 'No') 								   as third_party,
	ce.updated_at,
	cei_363.value 													   as achica_user,
	if(cei_367.value is null and cei_363.value is not null, timestamp(ce.created_at), cei_367.value) as achica_migration_date,
	cei_381.value 													   as cocosa_user,
	if(ced_382.value is null and cei_381.value is not null, timestamp(ce.created_at), ced_382.value) as cocosa_signup_at,
	greatest(
		ce.bq_last_processed_at, 
		cei.bq_last_processed_at, 
		cet_old_account.bq_last_processed_at,
		cei_s.bq_last_processed_at
	) as customer_bq_last_processed_at,
	greatest(
		ca_b_26.bq_last_processed_at, 
		ca_b_27.bq_last_processed_at, 
		ca_b_28.bq_last_processed_at, 
		ca_b_30.bq_last_processed_at, 
		ca_s_26.bq_last_processed_at, 
		ca_s_27.bq_last_processed_at, 
		ca_s_28.bq_last_processed_at, 
		ca_s_30.bq_last_processed_at
	) as address_bq_last_processed_at,
	ns.bq_last_processed_at as subscriber_bq_last_processed_at
from {{ ref('stg__customer_entity') }} ce
left join {{ ref('stg__customer_entity_int') }} cei
	on ce.entity_id = cei.entity_id
		and cei.attribute_id = 13
		and ce.ba_site = cei.ba_site
-- left join {{ ref('stg__customer_entity_varchar') }} cev_5
-- 	on ce.entity_id = cev_5.entity_id
-- 		and cev_5.attribute_id = 5
-- left join {{ ref('stg__customer_entity_varchar') }} cev_7
-- 	on ce.entity_id = cev_7.entity_id
--        	and cev_7.attribute_id = 7
left join {{ ref('stg__customer_entity_text') }} cet_old_account
	on ce.entity_id = cet_old_account.entity_id
       	and cet_old_account.attribute_id = 217
		and ce.ba_site = cet_old_account.ba_site
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_20
-- 	on cei.value = ca_b_20.entity_id
--        	and ca_b_20.attribute_id = 20
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_22
-- 	on cei.value = ca_b_22.entity_id
--        	and ca_b_22.attribute_id = 22
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_24
-- 	on cei.value = ca_b_24.entity_id
-- 		and ca_b_24.attribute_id = 24
-- left join {{ ref('stg__customer_address_entity_text') }} ca_b_25
-- 	on cei.value = ca_b_25.entity_id
--        	and ca_b_25.attribute_id = 25
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_26
	on cei.value = ca_b_26.entity_id
		and ca_b_26.attribute_id = 26
		and cei.ba_site = ca_b_26.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_27
	on cei.value = ca_b_27.entity_id
       	and ca_b_27.attribute_id = 27
		and cei.ba_site = ca_b_27.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_28
	on cei.value = ca_b_28.entity_id
       	and ca_b_28.attribute_id = 28
		and cei.ba_site = ca_b_28.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_30
	on cei.value = ca_b_30.entity_id
       	and ca_b_30.attribute_id = 30
		and cei.ba_site = ca_b_30.ba_site
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_31
-- 	on cei.value = ca_b_31.entity_id
--        	and ca_b_31.attribute_id = 31
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_b_32
-- 	on cei.value = ca_b_32.entity_id
--        	and ca_b_32.attribute_id = 32
left join {{ ref('stg__customer_entity_int') }} cei_s
	on ce.entity_id = cei_s.entity_id
       	and cei_s.attribute_id = 14
		and ce.ba_site = cei_s.ba_site
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_20
-- 	on cei_s.value = ca_s_20.entity_id
--        	and ca_s_20.attribute_id = 20
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_22
-- 	on cei_s.value = ca_s_22.entity_id
--        	and ca_s_22.attribute_id = 22
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_24
-- 	on cei_s.value = ca_s_24.entity_id
--        	and ca_s_24.attribute_id = 24
-- left join {{ ref('stg__customer_address_entity_text') }} ca_s_25
-- 	on cei_s.value = ca_s_25.entity_id
--        	and ca_s_25.attribute_id = 25
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_26
	on cei_s.value = ca_s_26.entity_id
       	and ca_s_26.attribute_id = 26
		and cei_s.ba_site = ca_s_26.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_27
	on cei_s.value = ca_s_27.entity_id
       	and ca_s_27.attribute_id = 27
		and cei_s.ba_site = ca_s_27.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_28
	on cei_s.value = ca_s_28.entity_id
       	and ca_s_28.attribute_id = 28
		and cei_s.ba_site = ca_s_28.ba_site
left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_30
	on cei_s.value = ca_s_30.entity_id
       	and ca_s_30.attribute_id = 30
		and cei_s.ba_site = ca_s_30.ba_site
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_31
-- 	on cei_s.value = ca_s_31.entity_id
--        	and ca_s_31.attribute_id = 31
-- left join {{ ref('stg__customer_address_entity_varchar') }} ca_s_32
-- 	on cei_s.value = ca_s_32.entity_id
--        	and ca_s_32.attribute_id = 32
left join (
	select customer_id, ba_site, subscriber_status, bq_last_processed_at 
	from {{ ref('stg__newsletter_subscriber') }}
	{% if is_incremental() %}
	where customer_id || '-' || ba_site in (select customer_id || '-' || ba_site from customers_updated)
	{% endif %}
	qualify row_number() over (partition by customer_id, ba_site order by subscriber_id desc) = 1
) ns
	on ce.entity_id = ns.customer_id and ce.ba_site = ns.ba_site
left join {{ ref('stg__customer_entity_int') }}	cei_222
	on ce.entity_id = cei_222.entity_id
       	and cei_222.attribute_id = 222
		and ce.ba_site = cei_222.ba_site
left join {{ ref('stg__customer_entity_int') }} cei_363
	on cei_363.entity_id = ce.entity_id
       	and cei_363.attribute_id = 363
       	and cei_363.value in (1,2)
		and ce.ba_site = cei_363.ba_site
left join {{ ref('stg__customer_entity_int') }} cei_381
	on cei_381.entity_id = ce.entity_id
       	and cei_381.attribute_id = 381
       	and cei_381.value in (1,2)
		and ce.ba_site = cei_381.ba_site
left join {{ ref('stg__customer_entity_datetime') }} ced_382
	on ced_382.entity_id = ce.entity_id
       	and ced_382.attribute_id = 382
		and ce.ba_site = ced_382.ba_site
left join {{ ref('stg__customer_entity_datetime') }} cei_367
	on cei_367.entity_id = ce.entity_id
		and cei_367.attribute_id = 367
		and ce.ba_site = cei_367.ba_site
where 1=1
{% if is_incremental() %}
	and ce.entity_id || '-' || ce.ba_site in (select customer_id || '-' || ba_site from customers_updated)
	and ce.bq_last_processed_at >= (select min(bq_last_processed_at) from customers_updated)
{% endif %}
)

select 
	ce.* except(dt_cr),
	coalesce(safe_cast(date(crds.date) as datetime), safe_cast(date(ce.achica_migration_date) as datetime), safe_cast(date(ce.cocosa_signup_at) as datetime), datetime(if(ce.ba_site = 'FR',timestamp(ce.dt_cr, "Europe/Paris"),timestamp(ce.dt_cr, "Europe/London")))) 	   as dt_cr
from customers ce
left join {{ ref('customers_record_data_source') }} crds 
        on ce.cst_id = crds.cst_id and ce.ba_site = crds.ba_site
