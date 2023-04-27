SELECT
    id,
    order_item_number,
    ba_site,
    sku,
    order_increment_id,
    qty,
    return_code,
    timestamp(created_at) as created_at,
    xml_created_at,
    exported_to_sap,
    wh_line_id,
    return_service,
    creditmemo_id,
    CAST(NULL AS DECIMAL(1)) AS maxdate
FROM
    {{ ref(
        'stg__bacore_stock_return'
    ) }}
