SELECT
    *
FROM
    {{ ref(
        'stg__sales_flat_shipment_item'
    ) }}
