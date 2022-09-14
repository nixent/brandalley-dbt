SELECT sha1(concat(sfo.increment_id, ifnull(CAST(sfc.refund_type_id AS STRING), '_'), ifnull(CAST(sfc.increment_id AS STRING), '_'))) as U_UNIQUE_ID, 
sfc.admin_user_id as adminUserId, sfo.increment_id as orderNumber, sfo.created_at as orderDate, sfc.created_at as dateOfStockReturned,
sfc.refund_type_id as refund_type_ID, (CASE CAST(sfc.state AS STRING) WHEN '1' THEN 'Pending' WHEN '2' THEN 'Refunded' WHEN '3' 
THEN 'cancelled' WHEN '4' THEN 'Approved - Pending Ogone' END) as refund_Status, sfc.created_at as creditMemoDate, 
sfc.increment_id as creditMemoNumber,sfc.grand_total as grandTotal, (CASE CAST(sfc.flag AS STRING) WHEN '0' THEN '' WHEN '1' THEN 'Require Attention' 
WHEN '2' THEN 'High Priority' END) as flag,sfc.approve_at as approvedAt, 
sfo.updated_at FROM {{ source('streamkap', 'sales_flat_creditmemo') }}  AS sfc INNER JOIN {{ source('streamkap', 'sales_flat_order') }}  AS sfo ON sfo.entity_id = sfc.order_id 
WHERE (sfo.sales_product_type != 12 or sfo.sales_product_type is null)