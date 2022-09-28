SELECT
    SHA1(
        CONCAT(IFNULL(cce.entity_id, '_'), IFNULL(cce.parent_id, '_'), IFNULL(ccei.entity_id, '_'))
    ) AS u_unique_id,
    cce.entity_id category_id,
    cce.parent_id parent_category_id,
    ccev.value category_name,
    ccev_parent.value parent_category_name,
    IF(
        ccei.value = 0,
        'No',
        'Yes'
    ) active,
    cce.path AS path,
    cce.level AS LEVEL,
    cce.updated_at
FROM
    {{ ref(
        'catalog_category_entity'
    ) }}
    cce
    LEFT JOIN     {{ ref(
        'catalog_category_entity_varchar'
    ) }}
    ccev
    ON cce.entity_id = ccev.entity_id
    AND ccev.value IS NOT NULL
    AND ccev.attribute_id = 41
    AND ccev.store_id = 0
    LEFT JOIN     {{ ref(
        'catalog_category_entity_varchar'
    ) }}
    ccev_parent
    ON cce.parent_id = ccev_parent.entity_id
    AND ccev_parent.attribute_id = 41
    AND ccev_parent.store_id = 0
    LEFT JOIN     {{ ref(
        'catalog_category_entity_int'
    ) }}
    ccei
    ON ccei.entity_id = cce.entity_id
    AND ccei.attribute_id = 42
    AND ccei.store_id = 0
