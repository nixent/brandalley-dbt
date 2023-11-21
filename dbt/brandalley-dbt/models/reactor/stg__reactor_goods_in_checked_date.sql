{{ config(materialized="table", tags=["job_daily"]) }}

--Essentially the same as goods in without removing whistl stock transfers and getting them from magento. This model is for units checked in at kettering whereas goods in is for aging purposes so includes the original arrived at (even if it is pre kettering)

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
    )
select
    purchase_id as po_id,
    magento_sku as sku,
    date_arrived,
    qty_arrived,
    unit_cost,
    qty_arrived * unit_cost as cost_arrived,
    supplier,
    company,
    stock_type
from kettering_goods_in_correction


