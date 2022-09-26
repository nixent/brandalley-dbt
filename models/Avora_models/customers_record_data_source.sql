SELECT
    ce.entity_id AS cst_id,
    cei.value AS record_data_source,
    ced.value AS DATE
FROM
    {{ source(
        'streamkap',
        'customer_entity'
    ) }}
    ce
    JOIN {{ source(
        'streamkap',
        'customer_entity_int'
    ) }}
    cei
    ON ce.entity_id = cei.entity_id
    JOIN {{ source(
        'streamkap',
        'customer_entity_datetime'
    ) }}
    ced
    ON ce.entity_id = ced.entity_id
WHERE
    cei.attribute_id = 381
    AND ced.attribute_id = 382
