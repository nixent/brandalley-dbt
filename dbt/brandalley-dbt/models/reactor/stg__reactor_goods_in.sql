{{ config(materialized="table", tags=["reactor_stock_daily"]) }}

with
    kettering_goods_in as (
        select
            psl.purchaseid as purchase_id,
            sl.legacy_id as magento_sku,
            cast(
                datetime_add('1970-01-01', interval psl.timestamp second) as date
            ) as date_arrived,
            sum(psl.newly_received_quantity) as qty_arrived,
        from {{ ref("stg__purchasestocklog") }} psl
        left join {{ ref("stg__stocklist") }} sl on psl.stockid = sl.id
        where length(cast(psl.purchaseid as string)) > 8
        group by 1, 2, 3
    ),
    pre_kettering as (
        select
            poi.po_id as purchase_id,
            poi.sku as magento_sku,
            cast(spgi.delivery_date as date) as date_arrived,
            sum(to_order) as qty_arrived
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
            kettering_goods_in kgi
            on cast(poi.po_id as string) = substring(
                cast(kgi.purchase_id as string),
                1,
                char_length(cast(kgi.purchase_id as string)) - 2
            )
            and poi.sku = kgi.magento_sku
        where kgi.purchase_id is null
        group by 1, 2, 3
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
    'purchasestock' as src
from kettering_goods_in
union all
select
    purchase_id as po_id,
    magento_sku as sku,
    date_arrived,
    qty_arrived,
    'magento' as src
from pre_kettering

