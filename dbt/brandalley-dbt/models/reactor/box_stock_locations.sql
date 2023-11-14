{{ config(materialized="table", tags=["job_daily"]) }}

with raw_stock_profile as (
select
    cast(current_date as date) as logged_date,
    bsi.stockid as reactor_sku_id,
    sl.legacy_id as sku,
    bsi.quantity as quantity,
    bsi.boxid as box_id,
    'available' as stock_type
from {{ ref("stg__boxstockindex") }} as bsi
left join {{ ref("stg__stocklist") }} sl on bsi.stockid = sl.id

union all

select
    cast(current_date as date) as logged_date,
    o.stockid as reactor_sku_id,
    sl.legacy_id as sku,
    o.quantity,
    obi.boxid as box_id,
    'allocated' as stock_type
from {{ ref("stg__orderboxindex") }} as obi
inner join {{ ref("stg__orders") }} as o on obi.orderid = o.orderid
left join {{ ref("stg__stocklist") }} sl on o.stockid = sl.id
where obi.__deleted = false
)
select
	rsp.logged_date,
	rsp.sku,
	max(rsp.reactor_sku_id) as reactor_sku_id,
    rsp.box_id,
	sum(rsp.quantity) as on_hand,
	sum(case when rsp.stock_type = 'allocated'
		then rsp.quantity
	    else 0 end) as allocated,
	sum(case when rsp.stock_type = 'available'
		then rsp.quantity
	    else 0 end) as available,
from raw_stock_profile as rsp
group by 1,2,4