SELECT
    SHA1(
        CONCAT(IFNULL(CAST(cce.entity_id AS STRING), '_'), IFNULL(CAST(cce.parent_id AS STRING), '_'), IFNULL(CAST(ccei.entity_id AS STRING), '_'))
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
    cce.updated_at,
    cce.ba_site
FROM
    {{ ref(
        'stg__catalog_category_entity'
    ) }}
    cce
    LEFT JOIN {{ ref(
        'stg__catalog_category_entity_varchar'
    ) }}
    ccev
    ON cce.entity_id = ccev.entity_id
    AND ccev.value IS NOT NULL
    AND ccev.attribute_id = 41
    AND ccev.store_id = 0
    and ccev.ba_site = cce.ba_site
    LEFT JOIN {{ ref(
        'stg__catalog_category_entity_varchar'
    ) }}
    ccev_parent
    ON cce.parent_id = ccev_parent.entity_id
    AND ccev_parent.attribute_id = 41
    AND ccev_parent.store_id = 0
    and ccev_parent.ba_site = cce.ba_site
    LEFT JOIN {{ ref(
        'stg__catalog_category_entity_int'
    ) }}
    ccei
    ON ccei.entity_id = cce.entity_id
    AND ccei.attribute_id = 42
    AND ccei.store_id = 0
    and cce.ba_site = ccei.ba_site