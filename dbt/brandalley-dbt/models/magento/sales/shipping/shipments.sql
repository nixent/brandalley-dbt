select
    sfs.entity_id,
    sfs.ba_site,
    sfs.store_id,
    sfs.total_weight,
    sfs.total_qty,
    sfs.email_sent,
    sfs.order_id,
    sfs.customer_id,
    sfs.shipping_address_id,
    sfs.billing_address_id,
    sfs.shipment_status,
    sfs.increment_id,
    cast(sfs.created_at as timestamp) as created_at,
    sfs.updated_at,
    sfs.packages,
    if(pdos.warehouse_id is not null, true, false) as is_wh_shipment
from {{ ref('stg__sales_flat_shipment') }} sfs
left join {{ ref('stg_uk__prism_dispatch_order_shipments') }} pdos
on sfs.entity_id = pdos.magento_shipment_id and sfs.ba_site = 'UK'
