SELECT
    sfc.ba_site || '-' || entity_id as ba_site_entity_id,
    entity_id,
    sfc.ba_site,
    store_id,
    adjustment_positive,
    base_shipping_tax_amount,
    store_to_order_rate,
    base_discount_amount,
    base_to_order_rate,
    grand_total,
    base_adjustment_negative,
    base_subtotal_incl_tax,
    shipping_amount,
    subtotal_incl_tax,
    adjustment_negative,
    base_shipping_amount,
    store_to_base_rate,
    base_to_global_rate,
    base_adjustment,
    base_subtotal,
    discount_amount,
    subtotal,
    adjustment,
    base_grand_total,
    base_adjustment_positive,
    base_tax_amount,
    shipping_tax_amount,
    tax_amount,
    order_id,
    email_sent,
    creditmemo_status,
    state,
    shipping_address_id,
    billing_address_id,
    invoice_id,
    store_currency_code,
    order_currency_code,
    base_currency_code,
    global_currency_code,
    transaction_id,
    increment_id,
    created_at,
    updated_at,
    hidden_tax_amount,
    base_hidden_tax_amount,
    shipping_hidden_tax_amount,
    base_shipping_hidden_tax_amnt,
    shipping_incl_tax,
    base_shipping_incl_tax,
    discount_description,
    base_customer_balance_amount,
    customer_balance_amount,
    bs_customer_bal_total_refunded,
    customer_bal_total_refunded,
    base_gift_cards_amount,
    gift_cards_amount,
    gw_base_price,
    gw_price,
    gw_items_base_price,
    gw_items_price,
    gw_card_base_price,
    gw_card_price,
    gw_base_tax_amount,
    gw_tax_amount,
    gw_items_base_tax_amount,
    gw_items_tax_amount,
    gw_card_base_tax_amount,
    gw_card_tax_amount,
    base_reward_currency_amount,
    reward_currency_amount,
    reward_points_balance,
    reward_points_balance_refund,
    refund_type_id,
    adjustment_reason_id,
    status_id,
    admin_user_id,
    approve_at,
    exported_to_sap,
    fee_type,
    flag,
    approved_by,
    canceled_at,
    is_batch,
    count_sku_refunded,
    qty_returned_to_warehouse,
    qty_refunded
FROM
    {{ ref(
        'stg__sales_flat_creditmemo'
    ) }} sfc
    left join (select   count(entity_id)                as count_sku_refunded,
                        sum(qty_returned_to_warehouse)  as qty_returned_to_warehouse,
                        sum(qty)                        as qty_refunded,
                        parent_id,
                        ba_site from
    {{ ref(
        'stg__sales_flat_creditmemo_item'
    ) }} group by parent_id, ba_site) sfci
    on sfci.parent_id = sfc.entity_id and sfc.ba_site = sfci.ba_site