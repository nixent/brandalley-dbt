{{ config(materialized="table", tags=["job_daily"]) }}

with
    kettering_goods_in as (
        select
            psl.purchaseid as purchase_id,
            sl.legacy_id as magento_sku,
            cast(
                datetime_add('1970-01-01', interval psl.timestamp second) as date
            ) as date_arrived,
            sum(psl.newly_received_quantity) as qty_arrived,
            sl.item_cost as unit_cost,
            d.name as supplier,
            d.company as company,
            if(d.potype = 'bk2bk', 'X Dock', 'Stock') as stock_type
        from {{ ref("stg__purchasestocklog") }} psl
        left join {{ ref("stg__stocklist") }} sl on psl.stockid = sl.id
        left join
            (
                select purchaseid, stockid, b.name, b.company, a.potype
                from {{ ref("stg__purchasestock") }} a
                left join {{ ref("stg__stockist") }} b on a.supplierid = b.stockistid
                group by 1, 2, 3, 4, 5
            ) d
            on psl.purchaseid = d.purchaseid
            and psl.stockid = d.stockid
        where length(cast(psl.purchaseid as string)) > 8
        group by 1, 2, 3, 5, 6, 7, 8
    ),
    negative_goods_in as (
        select
            *,
            lag(qty_arrived, 1) over (
                partition by purchase_id, magento_sku order by date_arrived desc
            ) as previous_qty_arrived
        from kettering_goods_in
    ),
    kettering_goods_in_correction as (
        select
            purchase_id,
            magento_sku,
            date_arrived,
            unit_cost,
            supplier,
            company,
            stock_type,
            case
                when previous_qty_arrived < 0
                then qty_arrived + previous_qty_arrived
                else qty_arrived
            end as qty_arrived
        from negative_goods_in a
        where a.qty_arrived > 0  -- this has been added as occassionally kettering check in more than they actually got and thus create a correction with a negative number. So here we are taking that negative number off previous row.
    ),
    pre_kettering as (
        select
            poi.po_id as purchase_id,
            poi.sku as magento_sku,
            ifnull(
                cast(spgi.delivery_date as date), cast(po.delivery_date as date)
            ) as date_arrived,
            sum(to_order) as qty_arrived,
            poi.cost_gbp
        from {{ ref("stg__catalog_product_po_item") }} poi
        inner join
            {{ ref("stg__catalog_product_po") }} po
            on poi.po_id = po.po_id
            and poi.ba_site = po.ba_site
        left join
            {{ ref("stg__stock_prism_grn") }} spg
            on cast(spg.purchase_order_reference as integer) = po.po_id
            and po.ba_site = spg.ba_site
        left join
            {{ ref("stg__stock_prism_grn_item") }} spgi
            on spgi.grn_id = spg.grn_id
            and spgi.sku = poi.sku
            and poi.ba_site = spgi.ba_site
        left join
            kettering_goods_in_correction kgic
            on cast(poi.po_id as string) = substring(
                cast(kgic.purchase_id as string),
                1,
                char_length(cast(kgic.purchase_id as string)) - 2
            )
            and poi.sku = kgic.magento_sku
        where kgic.purchase_id is null and ifnull(cast(spgi.delivery_date as date), cast(po.delivery_date as date)) < current_date
        group by 1, 2, 3, 5
    )
select
    cast(
        substring(
            cast(purchase_id as string), 1, char_length(cast(purchase_id as string)) - 2
        ) as int
    ) as po_id,
    magento_sku as sku,
    date_arrived,
    qty_arrived,
    unit_cost,
    qty_arrived * unit_cost as cost_arrived,
    'reactor' as src,
    'Kettering' as warehouse,
    supplier,
    company,
    stock_type
from kettering_goods_in_correction
union all
select
    purchase_id as po_id,
    magento_sku as sku,
    date_arrived,
    qty_arrived,
    cost_gbp,
    qty_arrived * cost_gbp as cost_arrived,
    'magento' as src,
    null as warehouse,
    null,
    null,
    null
from pre_kettering

