select ce.entity_id as cst_id, cei.value as record_data_source, ced.value as date
from {{ source('streamkap', 'customer_entity') }} ce
JOIN {{ source('streamkap', 'customer_entity_int') }} cei ON ce.entity_id = cei.entity_id
JOIN {{ source('streamkap', 'customer_entity_datetime') }} ced ON ce.entity_id = ced.entity_id
WHERE cei.attribute_id = 381 and ced.attribute_id=382