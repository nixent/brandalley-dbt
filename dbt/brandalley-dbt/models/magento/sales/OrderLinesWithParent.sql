SELECT
    brand_option.value brand,
    sfo.customer_id,
    sfo.increment_id,
    sfoi.product_type,
    sfoi.parent_item_id,
    brand_option.store_id,
    sfo.created_at,
    ns.subscriber_status,
    SUM(
        sfoi.qty_invoiced
    ) total_gbp,
    SUM((sfoi.qty_invoiced * sfoi2.base_price_incl_tax) - sfoi2.discount_amount) AS total_gbp_after_vouchers
FROM
    {{ ref('stg__sales_flat_order') }}
    sfo
    INNER JOIN {{ ref('stg__sales_flat_order_item') }}
    sfoi
    ON sfo.entity_id = sfoi.order_id
    LEFT JOIN {{ ref('stg__sales_flat_order_item') }}
    sfoi2
    ON sfo.entity_id = sfoi2.order_id
    AND sfoi.sku = sfoi2.sku
    AND sfoi2.parent_item_id IS NULL
    INNER JOIN {{ ref('stg__customer_entity') }}
    ce
    ON sfo.customer_id = ce.entity_id
    LEFT JOIN {{ ref('stg__catalog_product_super_link') }} AS parent_relation
    ON parent_relation.product_id = sfoi.product_id
    LEFT JOIN {{ ref('stg__catalog_product_entity_int') }} AS cpeib
    ON cpeib.attribute_id = 178
    AND cpeib.entity_id = CASE
        WHEN parent_relation.parent_id IS NULL THEN sfoi.product_id
        ELSE parent_relation.parent_id
    END
    LEFT OUTER JOIN {{ ref('stg__eav_attribute_option_value') }}
    brand_option
    ON cpeib.value = brand_option.option_id
    LEFT JOIN {{ ref('stg__newsletter_subscriber') }}
    ns
    ON ns.customer_id = sfo.customer_id {{ dbt_utils.group_by(8) }}
