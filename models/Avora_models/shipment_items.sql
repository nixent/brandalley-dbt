SELECT
    *
FROM
    {{ ref(
        'sales_flat_shipment_item'
    ) }}
