{{ config(materialized="table", tags=["job_daily"]) }}


select
    'Stock Exit' as movement_type,
    cast(datetime_add('1970-01-01', interval bscl.timestamp second) as date) as logged_date,
    cast(bscl.stockid as string) as reactor_sku,
    sl.legacy_id as magento_sku,
    sum(bscl.difference) as qty_change,
    sl.item_cost as unit_cost,
    sel.type as reason,
    sel.details as details
from {{ ref('stg__boxstockchecklog') }} bscl
left join (select boxstockchecklog_id, type, details from {{ ref('stg__stock_exit_ledger') }} group by 1,2,3) sel on bscl.id=sel.boxstockchecklog_id
left join {{ ref('stg__stocklist') }} sl on bscl.stockid=sl.id
where bscl.via='SE'
group by 1,2,3,4,6,7,8

union all

select
    'Manual Update' as movement_type,
    cast(datetime_add('1970-01-01', interval bscl.timestamp second) as date) as logged_date,
    cast(bscl.stockid as string) as reactor_sku,
    sl.legacy_id as magento_sku,
    sum(bscl.difference) as qty_change,
    sl.item_cost as unit_cost,
    cast(null as string),
    cast(null as string)
from {{ ref('stg__boxstockchecklog') }} bscl
left join {{ ref('stg__stocklist') }} sl on bscl.stockid=sl.id
where bscl.via='MU'
group by 1,2,3,4,6,7,8

union all

select 
    'SOR Stock Adjustment',
    date(a.updated_at) as logged_date,
    cast(a.stockid as string) as reactor_sku,
    sl.legacy_id as magento_sku,
    sum(a.quantity*-1) as qty_change,
    sl.item_cost as unit_cost,
    cast(null as string),
    cast(null as string)
from {{ ref('stg__orders') }} a
left join {{ ref('stg__stocklist') }} sl on a.stockid=sl.id
where customerid in (select customerid from {{ ref('stg__orders') }} where deliverytype='SOR')
group by 1,2,3,4,6,7,8

union all

select
    'Sample Order',
    logged_date,
    cast(reactor_sku as string) as reactor_sku,
    magento_sku,
    sum(qty_on_order) as qty_change,
    unit_cost,
    case when delivery_postcode='EC2A 4NW' then concat('Sample Office ', type) else concat('Sample Influencer ', type) end as reason,
    concat('Reactor Order ',cast(reactor_order_number as string)) as details
from {{ ref('sample_orders') }}
group by 1,2,3,4,6,7,8
limit 10000