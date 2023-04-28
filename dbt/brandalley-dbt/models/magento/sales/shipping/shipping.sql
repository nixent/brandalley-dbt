SELECT
    SHA1(
        CONCAT(
            sfsi.product_id,
            sfo.increment_id,
            sfsi.ba_site,
            IFNULL(CAST(sfsi.parent_id AS STRING), '_'),
            IFNULL(CAST(sfoa.entity_id AS STRING), '_'),
            IFNULL(CAST(sfs.order_id AS STRING), '_'),
            IFNULL(CAST(sfs.customer_id AS STRING), '_'),
            IFNULL(CAST(sfsi.sku AS STRING), '_'),
            IFNULL(CAST(sfsi.entity_id AS STRING), '_'),
            IFNULL(CAST(sfsi.sap_id AS STRING), '_')
        )
    ) as unique_id,
    sfsi.ba_site,
    sfsi.product_id,
    sfsi.sku,
    sfsi.qty,
    sfsi.weight,
    sfo.increment_id as order_id,
    sfs.customer_id,
    sfoa.postcode,
    sfs.increment_id as shipment_id,
    sfo.created_at as order_date,
    sfs.created_at as shipment_date,
    sfs.updated_at
FROM
    {{ ref(
        'stg__sales_flat_shipment_item'
    ) }}
    sfsi
    LEFT JOIN     {{ ref(
        'stg__sales_flat_shipment'
    ) }}
    sfs
    ON sfsi.parent_id = sfs.entity_id
    and sfsi.ba_site = sfs.ba_site
    LEFT JOIN     {{ ref(
        'stg__sales_flat_order'
    ) }}
    sfo
    ON sfs.order_id = sfo.entity_id
    and sfs.ba_site = sfo.ba_site
    LEFT JOIN     {{ ref(
        'stg__sales_flat_order_address'
    ) }}
    sfoa
    ON sfoa.entity_id = sfo.shipping_address_id
    and sfoa.ba_site = sfo.ba_site
WHERE
    (
        sfo.sales_product_type != 12
        OR sfo.sales_product_type IS NULL
    )