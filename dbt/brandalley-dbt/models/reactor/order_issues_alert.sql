{{ config(materialized="table") }}

select o.orderid, o.customerid, s.legacy_id as sku, po.ext_order_number
from {{ ref("stg__orders") }} o
left join {{ ref("stg__customers") }} c on c.customerid = o.customerid
inner join {{ ref("stg__stocklist") }} s on s.id = o.stockid
inner join {{ ref("stg__paas_orders") }} po on po.customer_id = o.customerid
where
    o.dispatched is null
    and c.completed = 'N'
    and o.completed_timestamp is null
    and o.status is null
    and not exists (
        select obi.id
        from {{ ref("stg__orderboxindex") }} obi
        where obi.orderid = o.orderid
    )
    and not exists (
        select o2.orderid
        from {{ ref("stg__orders") }} o2
        where o2.referenceid = o.orderid
    )
    and o.carrier_service_identifier is null
    and o.dispatch_warehouseid = 5
