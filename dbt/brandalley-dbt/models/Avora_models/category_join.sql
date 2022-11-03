SELECT
    cp.product_id,
    cp.category_id,
    p.updated_at
FROM
    {{ ref(
        'stg__catalog_category_product'
    ) }}
    cp
    INNER JOIN     {{ ref(
        'stg__catalog_product_entity'
    ) }}
    p
    ON cp.product_id = p.entity_id
