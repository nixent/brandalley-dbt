{{ config(materialized="table", tags=["reactor_stock_daily"]) }}

with raw_stock_profile as (
select
    cast(current_date as date) as logged_date,
    cast(bsi.stockid as string) as reactor_sku_id,
    sl.legacy_id as sku,
    bsi.quantity as quantity,
    cast(bsi.boxid as string) as box_id,
    'available' as stock_type
from {{ ref("stg__boxstockindex") }} as bsi
left join {{ ref("stg__stocklist") }} sl on bsi.stockid = sl.id

union all

select
    cast(current_date as date) as logged_date,
    cast(o.stockid as string) as reactor_sku_id,
    sl.legacy_id as sku,
    o.quantity,
    cast(obi.boxid as string) as box_id,
    'allocated' as stock_type
from {{ ref("stg__orderboxindex") }} as obi
inner join {{ ref("stg__orders") }} as o on obi.orderid = o.orderid
left join {{ ref("stg__stocklist") }} sl on o.stockid = sl.id
where obi.__deleted = false


union all

select cast(current_date as date) as logged_date,
       cast(ubsi.stockid as string) as reactor_sku_id,
       sl.legacy_id as sku,
       ubsi.quantity as quantity,
       cast(ubsi.boxid as string) as box_id, 
       'unsellable' as stock_type       
from {{ ref("stg__unsellableboxstockindex") }} ubsi
left join {{ ref("stg__box") }} as b on ubsi.boxid = b.id
left join {{ ref("stg__boxracking") }} as br on br.id = b.boxrackingid
left join {{ ref("stg__adboxaisle") }} as aba on aba.aisleid = br.aisleid
left join {{ ref("stg__adboxzone") }} as abz on abz.zoneid = aba.zoneid
left join {{ ref("stg__stocklist") }} sl on ubsi.stockid = sl.id
where abz.name <> 'z Bulk Zone GI Bay'
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
    sum(case when rsp.stock_type = 'unsellable' and rsp.box_id not in ('-32', '-33', '-34', '-35', '-41', '-42')
		then rsp.quantity
	    else 0 end) as unsellable_good,
    sum(case when rsp.stock_type = 'unsellable' and rsp.box_id in ('-32', '-33', '-34', '-35', '-41', '-42')
		then rsp.quantity
	    else 0 end) as unsellable_bad,
from raw_stock_profile as rsp
group by 1,2,4
