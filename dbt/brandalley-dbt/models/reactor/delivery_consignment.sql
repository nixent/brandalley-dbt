{{ config(materialized="table", tags=["job_daily"]) }}

SELECT b.customerid, 
       e.ext_order_id,
	   f.productid as reactor_pid,
	   f.productname,
	   aa.quantity as units_qty,
	   b.id AS delivery_consignment_id,
	   ifnull(e.carrier_service_identifier,c.title) AS delivery_type,
	   cast(b.created as date) AS created_at,
	   cast(e.leftwarehouse_timestamp as date) as left_warehouse_date,
	   cast(e.ship_by as date) as ship_by,
	   b.charge,
	   a.id as package_id,
	   a.weight,
	   a.length,
	   a.width,
	   a.height,
	   a.package_sizeid,
	   case when date(leftwarehouse_timestamp)<>'1970-01-01' then datetime_diff(e.leftwarehouse_timestamp, b.created, hour) else null end as dispatch_time_hours,
	   case when date(leftwarehouse_timestamp)<>'1970-01-01' and leftwarehouse_timestamp>b.created then datetime_diff(e.leftwarehouse_timestamp, b.created, day) 
			when date(leftwarehouse_timestamp)<>'1970-01-01' and leftwarehouse_timestamp<b.created then 0 else null end as dispatch_time_days,
	   case when date(leftwarehouse_timestamp)<>'1970-01-01' then 'Shipped' else 'Unshipped' end as status,
	   case when date(leftwarehouse_timestamp)<cast(ship_by as date) then 'Early'
			when date(leftwarehouse_timestamp)>cast(ship_by as date) then 'Late' else 'On Time' end as shipped_on_time,
	   case when date(leftwarehouse_timestamp)<>'1970-01-01' then null else date_diff(current_date, date(b.created), day) end as undispatched_days_old,
	   if(date(leftwarehouse_timestamp)='1970-01-01' and cast(e.ship_by as date)<current_date, True, False)  as late_unshipped_flag,
	   case when e.priority='E' then 'Standard' when e.priority='P' then 'Express' when e.priority='S' then 'Standard' else 'Other' end as service_level,
	   b.delpostcode as ship_to_postcode,
	   a.packagecode,
	   g.site,
	   j.legacy_id as magento_sku,
	   j.item_cost as unit_cost
FROM {{ ref('stg__deliveryproduct') }} aa
left join {{ ref('stg__deliverypackage') }} a on aa.deliverypackageid=a.id
LEFT JOIN {{ ref('stg__delivery_consignment') }} b ON a.deliveryconsignmentid=b.id
LEFT JOIN {{ ref('stg__deliverytypes') }} c ON b.deliverytype=c.code
left join {{ ref('stg__orders') }} e on aa.orderid=e.orderid
left join {{ ref('stg__product') }} f on e.productid=f.productid
left join {{ ref('stg__customers') }} g on b.customerid=g.customerid
left join {{ ref('stg__stocklist') }} j on e.stockid=j.id
WHERE b.created is not null
