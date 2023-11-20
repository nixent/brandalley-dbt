{{ config(materialized="table", tags=["job_daily"]) }}


select
    cast(a.id as string) as reactor_sku,
    a.barcode,
    cast(b.productid as string) as productid,
    b.productname,
    a.legacy_id as magento_sku,
    item_cost as unit_cost
from {{ ref("stg__stocklist") }} a
inner join {{ ref("stg__product") }} b on a.productid = b.productid
