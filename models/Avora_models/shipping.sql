SELECT
    SHA1(
        CONCAT(
            sfsi.product_id,
            sfo.increment_id,
            IFNULL(CAST(sfsi.parent_id AS STRING), '_'),
            IFNULL(CAST(sfoa.entity_id AS STRING), '_'),
            IFNULL(CAST(sfs.order_id AS STRING), '_'),
            IFNULL(CAST(sfs.customer_id AS STRING), '_'),
            IFNULL(CAST(sfsi.sku AS STRING), '_'),
            IFNULL(CAST(sfsi.entity_id AS STRING), '_'),
            IFNULL(CAST(sfsi.sap_id AS STRING), '_')
        )
    ) unique_id,
    sfsi.product_id,
    sfsi.sku,
    sfsi.qty,
    sfsi.weight,
    sfo.increment_id order_id,
    sfs.customer_id,
    sfoa.postcode,
    sfs.increment_id shipment_id,
    sfo.created_at order_date,
    IFNULL(
        sfs.created_at,
        '0000-00-00 00:00:00'
    ) shipment_date,
    sfs.updated_at
FROM
    {{ source(
        'streamkap',
        'sales_flat_shipment_item'
    ) }}
    sfsi
    LEFT JOIN {{ source(
        'streamkap',
        'sales_flat_shipment'
    ) }}
    sfs
    ON sfsi.parent_id = sfs.entity_id
    LEFT JOIN {{ source(
        'streamkap',
        'sales_flat_order'
    ) }}
    sfo
    ON sfs.order_id = sfo.entity_id
    LEFT JOIN {{ source(
        'streamkap',
        'sales_flat_order_address'
    ) }}
    sfoa
    ON sfoa.entity_id = sfo.shipping_address_id
WHERE
    (
        sfo.sales_product_type != 12
        OR sfo.sales_product_type IS NULL
    )
