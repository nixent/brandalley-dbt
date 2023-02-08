SELECT 
cpn.date_comp_exported AS po_date, 
cpn.negotiation_id, 
sfoi.sku, 
sfo.increment_id AS order_id, 
sfoi.qty_backordered AS consigment_ordered, 
sfoi.created_at,
eaov.value AS brand, 
cpev.value AS name, 
cpr.reference AS reference_ref, 
cpr.supplier_id, 
cpn.sap_ref
FROM {{ ref(
        'stg__catalog_product_negotiation')
        }} 
        cpn
INNER JOIN {{ ref(
        'stg__sales_flat_order_item')
        }} 
        sfoi ON (
cpn.negotiation_id = sfoi.nego AND
sfoi.created_at > cpn.date_comp_exported AND
sfoi.product_type = 'simple' AND
cpn.status = 70 AND
sfoi.qty_backordered > 0
)
JOIN {{ ref(
        'stg__sales_flat_order')
        }} 
        sfo ON sfo.entity_id = sfoi.order_id
LEFT JOIN {{ ref(
        'stg__catalog_product_reference')
        }}
        cpr ON cpr.entity_id = sfoi.product_id
LEFT JOIN {{ ref(
        'stg__sales_flat_order_item')
        }} 
        sfoii ON ( sfoi.parent_item_id = sfoii.parent_item_id)
LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int')
        }}
        cpei ON (sfoii.product_id = cpei.entity_id AND cpei.attribute_id = 178)
LEFT JOIN {{ ref(
        'stg__catalog_product_entity_varchar')
        }} 
        cpev ON (sfoii.product_id = cpev.entity_id AND cpev.attribute_id = 71)
LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value')
        }} 
        eaov ON cpei.value = eaov.option_id