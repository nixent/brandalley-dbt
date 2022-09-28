SELECT
    SHA1(
        CONCAT(
            sfo.increment_id,
            IFNULL(CAST(sfc.refund_type_id AS STRING), '_'),
            IFNULL(CAST(sfc.increment_id AS STRING), '_')
        )
    ) AS u_unique_id,
    sfc.admin_user_id AS adminUserId,
    sfo.increment_id AS orderNumber,
    sfo.created_at AS orderDate,
    sfc.created_at AS dateOfStockReturned,
    sfc.refund_type_id AS refund_type_ID,
    (
        CASECAST(sfc.state AS STRING)
        WHEN '1' THEN 'Pending'
        WHEN '2' THEN 'Refunded'
        WHEN '3' THEN 'cancelled'
        WHEN '4' THEN 'Approved - Pending Ogone'END
    ) AS refund_Status,
    sfc.created_at AS creditMemoDate,
    sfc.increment_id AS creditMemoNumber,
    sfc.grand_total AS grandTotal,
    (
        CASECAST(sfc.flag AS STRING)
        WHEN '0' THEN ''
        WHEN '1' THEN 'Require Attention'
        WHEN '2' THEN 'High Priority'END
    ) AS flag,
    sfc.approve_at AS approvedAt,
    sfo.updated_at
FROM
    {{ ref(
        'sales_flat_creditmemo'
    ) }} AS sfc
    INNER JOIN     {{ ref(
        'sales_flat_order'
    ) }} AS sfo
    ON sfo.entity_id = sfc.order_id
WHERE
    (
        sfo.sales_product_type != 12
        OR sfo.sales_product_type IS NULL
    )
