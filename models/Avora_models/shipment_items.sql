SELECT
    *
FROM
    {{ source(
        'streamkap',
        'sales_flat_shipment_item'
    ) }}
