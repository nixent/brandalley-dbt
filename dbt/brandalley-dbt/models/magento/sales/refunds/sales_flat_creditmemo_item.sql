SELECT
    cti.ba_site || '-' || cti.entity_id as ba_site_entity_id,
    cti.entity_id,
    cti.ba_site,
    cti.parent_id,
    cti.base_price,
    cti.tax_amount,
    cti.base_row_total,
    cti.discount_amount,
    cti.row_total,
    cti.base_discount_amount,
    cti.price_incl_tax,
    cti.base_tax_amount,
    cti.base_price_incl_tax,
    cti.qty,
    cti.base_cost,
    cti.price,
    cti.base_row_total_incl_tax,
    cti.row_total_incl_tax,
    cti.product_id,
    cti.order_item_id,
    cti.additional_data,
    cti.description,
    cti.sku,
    cti.name,
    cti.hidden_tax_amount,
    cti.base_hidden_tax_amount,
    cti.weee_tax_disposition,
    cti.weee_tax_row_disposition,
    base_weee_tax_disposition,
    base_weee_tax_row_disposition,
    weee_tax_applied,
    base_weee_tax_applied_amount,
    base_weee_tax_applied_row_amnt,
    cti.weee_tax_applied_amount,
    cti.weee_tax_applied_row_amount,
    cti.customer_refund_request,
    cti.exported_to_sap,
    cti.qty_returned_to_warehouse,
    timestamp(cm.created_at) created_at,
    timestamp(cm.updated_at) updated_at,
    cm.order_id,
    case 
        when cm.approve_at is not null then 'approved' 
        when cm.canceled_at is not null then 'canceled'
    else 'pending'
    end as refund_status,
    CASE
        cti.refund_reason_id
        WHEN 1 THEN 'Do not like/different to picture'
        WHEN 2 THEN 'Colour inappropriate'
        WHEN 3 THEN 'Material inappropriate'
        WHEN 4 THEN 'Item too small'
        WHEN 5 THEN 'Item too large'
        WHEN 6 THEN 'Quality inappropriate'
        WHEN 7 THEN 'Arrived too late'
        WHEN 8 THEN 'Item not ordered'
        WHEN 9 THEN 'Supplier manufacturing defect'
        WHEN 10 THEN 'Item broken or damaged'
        WHEN 11 THEN 'Customer not specified'
        WHEN 101 THEN 'Cancellation'
        WHEN 102 THEN 'Out of stock'
        WHEN 103 THEN 'Lost'
        WHEN 104 THEN 'Returned to sender'
        ELSE 'NOT DEFINED'
    END AS refund_reason
FROM
    {{ ref(
        'stg__sales_flat_creditmemo_item'
    ) }}
    cti
    INNER JOIN     {{ ref(
        'stg__sales_flat_creditmemo'
    ) }}
    cm
    ON cti.parent_id = cm.entity_id
    and cti.ba_site = cm.ba_site
