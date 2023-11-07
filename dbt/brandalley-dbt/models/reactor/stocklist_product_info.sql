{{ config(materialized="table") }}


select
    a.id as reactor_sku,
    a.barcode,
    b.productid,
    b.productname,
    a.legacy_id as magento_sku,
    item_cost as unit_cost
from {{ ref("stg__stocklist") }} a
inner join {{ ref("stg__product") }} b on a.productid = b.productid
