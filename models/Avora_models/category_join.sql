SELECT
    cp.product_id,
    cp.category_id,
    p.updated_at
FROM
    {{ source(
        'streamkap',
        'catalog_category_product'
    ) }}
    cp
    INNER JOIN {{ source(
        'streamkap',
        'catalog_product_entity'
    ) }}
    p
    ON cp.product_id = p.entity_id
