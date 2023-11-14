{{ config(materialized="table") }}


select 
    cast(datetime_add('1970-01-01', interval bscl.timestamp second) as date) as logged_date,
    cast(bscl.stockid as string) as reactor_sku,
    sl.legacy_id as magento_sku,
    sum(bscl.difference) as qty_exited,
    sl.item_cost as unit_cost,
    sel.type as exit_route,
    sel.details as exit_details
from {{ ref('stg__boxstockchecklog') }} bscl
left join (select boxstockchecklog_id, type, details from {{ ref('stg__stock_exit_ledger') }} group by 1,2,3) sel on bscl.id=sel.boxstockchecklog_id
left join {{ ref('stg__stocklist') }} sl on bscl.stockid=sl.id
where bscl.via='SE'
group by 1,2,3,5,6,7
