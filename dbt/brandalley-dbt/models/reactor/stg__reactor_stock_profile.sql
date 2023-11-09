{{ config(materialized="table", tags=["reactor_stock_daily"]) }}

with raw_stock_profile as (
select
    cast(current_date as date) as logged_date,
    bsi.stockid as reactor_sku_id,
    sl.legacy_id as sku,
    bsi.quantity as quantity,
    bsi.boxid as box_id,
    br.id as rack_id,
    br.row as rack_row,
    br.rack as rack,
    br.racknumber as rack_number,
    aba.aisleid as aisle_id,
    aba.aislenumber as aisle_number,
    abz.zoneid as zone_id,
    abz.name as zone_name,
    abz.abbr as zone_abbr,
    abz.color as zone_colour,
    abz.pickable as is_zone_pickable,
    'available' as stock_type
from {{ ref("stg__boxstockindex") }} as bsi
left join {{ ref("stg__box") }} as b on bsi.boxid = b.id
left join {{ ref("stg__boxracking") }} as br on br.id = b.boxrackingid
left join {{ ref("stg__adboxaisle") }} as aba on aba.aisleid = br.aisleid
left join {{ ref("stg__adboxzone") }} as abz on abz.zoneid = aba.zoneid
left join {{ ref("stg__stocklist") }} sl on bsi.stockid = sl.id

union all

select
    cast(current_date as date) as logged_date,
    o.stockid as reactor_sku_id,
    sl.legacy_id as sku,
    o.quantity,
    obi.boxid as box_id,
    br.id as rack_id,
    br.row as rack_row,
    br.rack as rack,
    br.racknumber as rack_number,
    aba.aisleid as aisle_id,
    aba.aislenumber as aisle_number,
    abz.zoneid as zone_id,
    abz.name as zone_name,
    abz.abbr as zone_abbr,
    abz.color as zone_colour,
    abz.pickable as is_zone_pickable,
    'allocated' as stock_type
from {{ ref("stg__orderboxindex") }} as obi
inner join {{ ref("stg__orders") }} as o on obi.orderid = o.orderid
left join {{ ref("stg__box") }} as b on obi.boxid = b.id
left join {{ ref("stg__boxracking") }} as br on br.id = b.boxrackingid
left join {{ ref("stg__adboxaisle") }} as aba on aba.aisleid = br.aisleid
left join {{ ref("stg__adboxzone") }} as abz on abz.zoneid = aba.zoneid
left join {{ ref("stg__stocklist") }} sl on o.stockid = sl.id
where obi.__deleted = false

union all

select
    cast(current_date as date) as logged_date,
    o.stockid as reactor_sku_id,
    sl.legacy_id as sku,
    sum(o.quantity) as quantity,
    null as box_id,
    null as rack_id,
    null as rack_row,
    null as rack,
    null as rack_number,
    null as aisle_id,
    null as aisle_number,
    null as zone_id,
    null as zone_name,
    null as zone_abbr,
    null as zone_colour,
    0 as is_zone_pickable,
    'pending dispatch' as stock_type
from {{ ref("stg__orders") }} as o
inner join
    {{ ref("stg__customers") }} as c
    on c.customerid = o.customerid
    and c.deleted is null
left join
    (select * from {{ ref("stg__orderboxindex") }} where __deleted = false) obi
    on obi.orderid = o.orderid
left join {{ ref("stg__stocklist") }} sl on o.stockid = sl.id
where
    o.completed_timestamp is not null
    and o.leftwarehouse_timestamp < '2000-01-01'
    and obi.id is null
group by o.stockid, sl.legacy_id

union all

select cast(current_date as date) as logged_date,
       ubsi.stockid as reactor_sku_id,
       sl.legacy_id as sku,
       ubsi.quantity as quantity,
       ubsi.boxid as box_id, 
       br.id as rack_id,
       br.row as rack_row,
       br.rack as rack,
       br.racknumber as rack_number,
       aba.aisleid as aisle_id,
       aba.aislenumber as aisle_number,
       abz.zoneid as zone_id,
       abz.name as zone_name,
       abz.abbr as zone_abbr,
       abz.color as zone_colour,
       abz.pickable as is_zone_pickable,
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
	sum(rsp.quantity) as on_hand,
	sum(case when rsp.stock_type in ('allocated', 'pending dispatch')
		then rsp.quantity
	    else 0 end) as allocated,
	sum(case when rsp.stock_type = 'available'
		then rsp.quantity
	    else 0 end) as available,
    sum(case when rsp.stock_type = 'unsellable'
		then rsp.quantity
	    else 0 end) as unsellable,
    sum(case when rsp.stock_type = 'unsellable' and rsp.box_id not in (-32, -33, -34, -35, -41, -42)
		then rsp.quantity
	    else 0 end) as unsellable_good,
    sum(case when rsp.stock_type = 'unsellable' and rsp.box_id in (-32, -33, -34, -35, -41, -42)
		then rsp.quantity
	    else 0 end) as unsellable_bad
from raw_stock_profile as rsp
group by 1,2
