{{ config(materialized="table", tags=["job_daily"]) }}

with orders as (
select 
  c.customerid as reactor_customer_id,
  aa.orderid as reactor_order_id,
  d.stockid,
  b.created, 
  aa.quantity,
  b.deliverytype as consignment_dtype,
  e.short_name as consignment_dtype_name,
  d.carrier_service_identifier as orders_carrier_service,
  d.deliverytype as orders_deliverytype,
  b.delpostcode,
  c.dpostcode,
  d.leftwarehouse_timestamp
FROM {{ ref('stg__deliveryproduct') }} aa
left join {{ ref('stg__deliverypackage') }} a on aa.deliverypackageid=a.id
LEFT JOIN {{ ref('stg__delivery_consignment') }} b ON a.deliveryconsignmentid=b.id
left join {{ ref('stg__customers') }} c on b.customerid=c.customerid
left join {{ ref('stg__orders') }} d on aa.orderid=d.orderid
left join {{ ref('stg__deliverytypes') }} e on b.deliverytype=e.code
where c.siteid=659 and d.deliverytype<>'SOR' and b.status=4
)
select 
    'Order' as type,
    a.customerid as reactor_order_number,
    a.stockid as reactor_sku,
    c.legacy_id as magento_sku,
    sum(a.quantity*-1) as qty_on_order,
    date(a.leftwarehouse_timestamp) as logged_date,
    a.deliverytype as deliverytype,
    a.carrier_service_identifier as carrier_service,
    b.dpostcode as delivery_postcode,
    c.item_cost as unit_cost
from {{ ref('stg__orders') }} a
left join {{ ref('stg__customers') }} b on a.customerid=b.customerid
left join {{ ref('stg__stocklist') }} c on a.stockid=c.id
where orderid in (select reactor_order_id from orders)
group by 1,2,3,4,6,7,8,9,10

union all

select
    'Return' as type,
    a.customerid as reactor_order_number,
    a.stockid as reactor_sku,
    c.legacy_id as magento_sku,
    sum(a.quantity*-1) as qty_on_order,
    date(a.updated_at) as logged_date,
    a.deliverytype as deliverytype,
    a.carrier_service_identifier as carrier_service,
    b.dpostcode as delivery_postcode,
    c.item_cost as unit_cost
from {{ ref('stg__orders') }} a
left join {{ ref('stg__customers') }} b on a.customerid=b.customerid
left join {{ ref('stg__stocklist') }} c on a.stockid=c.id
where a.referenceid in (select reactor_order_id from orders)
group by 1,2,3,4,6,7,8,9,10