{{ config(materialized="table", tags=["job_daily"]) }}


with po_info as (
select
    a.purchaseid,
    a.stockid,
    c.legacy_id as magento_sku,
    d.productname as productname,
    date(timestamp_seconds(a.ordertime)) as created_date,
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
qualify row_number() over (
                partition by purchaseid, stockid order by 3,4,5,6,7,8,9,10
            )
            = 1
)
select
    cast(a.purchaseid as string) as po_number,
    cast(a.stockid as string) as reactor_sku,
    date(timestamp_seconds(a.timestamp)) as arrival_date,
    sum(newly_received_quantity) as newly_received_qty,
    sum(newly_received_quantity*unit_cost) as newly_received_value,
    magento_sku,
    productname,
    created_date,
    status,
    supplier,
    stock_type, 
    unit_cost,
    whistl_stock_type
from {{ ref("stg__purchasestocklog") }} a
left join po_info b on a.purchaseid=b.purchaseid and a.stockid=b.stockid
group by 1,2,3,6,7,8,9,10,11,12,13
