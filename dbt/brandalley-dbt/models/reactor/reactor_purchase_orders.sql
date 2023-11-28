{{ config(materialized="table", tags=["job_daily"]) }}

with
    arrived_date as (
        select
            purchaseid,
            stockid,
            date(timestamp_seconds(timestamp)) as arrival_date,
            sum(newly_received_quantity) over (
                partition by purchaseid, stockid, date(timestamp_seconds(timestamp))
            ) as sum_of_arrived
        from {{ ref("stg__purchasestocklog") }}
    ),
    grouped as (
        select purchaseid, stockid, arrival_date, sum_of_arrived
        from arrived_date
        group by 1, 2, 3, 4
        qualify
            row_number() over (
                partition by purchaseid, stockid order by sum_of_arrived desc
            )
            = 1
    )
select
    cast(a.purchaseid as string) as po_number,
    cast(a.stockid as string) as reactor_sku,
    c.legacy_id as magento_sku,
    d.productname as productname,
    date(timestamp_seconds(a.ordertime)) as created_date,
    e.arrival_date as arrived_date,
    case
        when a.orderquantity = a.receivedquantity
        then 'fulfilled'
        when a.orderquantity < a.receivedquantity
        then 'overfulfilled'
        when a.orderquantity > a.receivedquantity and a.receivedquantity <> 0
        then 'part fulfilled'
        when a.receivedquantity = 0
        then 'on order'
    end as status,
    a.orderquantity as qty_ordered,
    a.receivedquantity as qty_received,
    a.orderquantity-a.receivedquantity as qty_outstanding,
    a.orderquantity*c.item_cost as value_ordered,
    a.receivedquantity*c.item_cost as value_received,
    (a.orderquantity-a.receivedquantity)*c.item_cost as value_outstanding,
    b.name as supplier,
    case when a.potype = 'bk2bk' then 'X Dock' else 'Stock' end as stock_type,
    c.item_cost as unit_cost,
    case
        when a.name like 'BA_Exit%' and length(cast(a.purchaseid as string)) < 8
        then 'Whistl Stock Move'
        when
            (
                a.name like '%drop%'
                or a.name like '%Drop%'
                or a.name like 'Whist_transfer%'
            )
            and length(cast(a.purchaseid as string)) < 8
        then 'Quarantine Receipts - Whistl'
        when a.name like 'Whist_returns%' and length(cast(a.purchaseid as string)) < 8
        then 'Whistl Returns Sorting'
        else 'Magento PO'
    end as whistl_stock_type
from {{ ref("stg__purchasestock") }} a
left join {{ ref("stg__stockist") }} b on a.supplierid = b.stockistid
left join {{ ref("stg__stocklist") }} c on a.stockid = c.id
left join {{ ref("stg__product") }} d on c.productid = d.productid
left join grouped e on a.purchaseid = e.purchaseid and a.stockid = e.stockid
