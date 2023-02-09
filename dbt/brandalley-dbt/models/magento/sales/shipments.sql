SELECT
    entity_id,
    store_id,
    total_weight,
    total_qty,
    email_sent,
    order_id,
    customer_id,
    shipping_address_id,
    billing_address_id,
    shipment_status,
    increment_id,
    cast(created_at as timestamp) as created_at,
    updated_at,
    packages
FROM
    {{ ref(
        'stg__sales_flat_shipment'
    ) }}