{{ config(materialized="table", tags=["job_daily"]) }}

select
    a.customerid,
    a.quantity,
    case when a.completed is null then 'Outstanding' else 'Returned' end as completed,
    cast(timestamp_seconds(a.received) as date) as received_at_date,
    a.resaleable_qty,
    a.damaged_qty,
    cast(timestamp_seconds(a.timestamp) as date) as created_at_date,
    date(b.ordered_timestamp) as order_posting_date,
    substring(
        a.explanation,
        strpos(a.explanation, '-') + 2,
        length(a.explanation) - strpos(a.explanation, '-') + 1
    ) as return_reason,
    b.ext_order_id,
    d.id as reactor_sku,
    d.legacy_id as magento_sku,
    e.productname as product_name,
    d.item_cost as unit_cost,
    g.tracking_number
from {{ ref("stg__returns") }} a
left join {{ ref("stg__orders") }} b on a.orderid = b.orderid
left join {{ ref("stg__stocklist") }} d on b.stockid = d.id
left join {{ ref("stg__product") }} e on d.productid = e.productid
left join {{ ref("stg__paas_returns") }} g on a.id = g.return_id
qualify row_number() over (partition by a.orderid order by a.id) = 1
