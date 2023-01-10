SELECT
       sfo.increment_id,
       sfo.entity_id AS magentoID,
       sfo.store_id,
       sfo.billing_address_id,
       sfo.shipping_address_id,
       sfo.subtotal_incl_tax,
       sfo.subtotal AS subtotal_excl_tax,
       sfo.discount_amount total_discount_amount,
       sfo.base_free_shipping_amount AS shipping_discount_amount,
       sfo.tax_amount AS total_tax,
       sfo.shipping_amount AS shipping_excl_tax,
       sfo.base_shipping_incl_tax AS shipping_incl_tax,
       sfo.grand_total,
       IF (
              sfo.total_paid IS NULL,
              0,
              sfo.total_paid
       ) AS total_paid,
       COALESCE(
              sfo.total_refunded,
              0
       ) total_refunded,
       sfo.total_due,
       sfo.base_total_invoiced_cost AS total_invoiced_cost,
       sfo.base_grand_total,
       sfo.status,
       sfo.coupon_rule_name,
       sfo.coupon_code,
       IF (
              sfop.method = 'braintreevzero',
              sfop.cc_type,
              sfop.method
       ) AS method,
       sfo.shipping_method,
       sfo.shipping_description,
       sfo.customer_id,
       '' AS customer_name,
       '' AS customer_phone,
       '' AS delivery_address,
       sfoa.postcode AS delivery_postcode,
       '' AS customer_age,
       sfo.expected_delivery_date,
       sfo.expected_delivery_days,
       cast(sfo.created_at as timestamp) AS created_at,
       sfo.updated_at,
       CASE WHEN sfo.status <> 'canceled' THEN row_number() OVER(  PARTITION BY sfo.customer_id, sfo.status <> 'canceled' ORDER BY sfo.increment_id) ELSE NULL END AS orderno,
       sfo.total_qty_ordered,
       ce.email,
       sfo.customer_firstname,
       sfo.customer_lastname,
       cc_trans_id, 
       additional_information,
       coalesce(TIMESTAMP_DIFF(cast(sfo.created_at as timestamp), lag(cast(sfo.created_at as timestamp)) over (partition by sfo.customer_id order by cast(sfo.created_at as timestamp)), day),0) as interval_between_orders
FROM
       {{ ref(
              'stg__sales_flat_order'
       ) }}
       sfo
       LEFT JOIN {{ ref(
              'stg__sales_flat_order_address'
       ) }}
       sfoa
       ON sfoa.entity_id = sfo.shipping_address_id
       LEFT JOIN {{ ref(
              'stg__sales_flat_order_address'
       ) }}
       sfoa_b
       ON sfoa_b.entity_id = sfo.billing_address_id
       LEFT JOIN {{ ref(
              'stg__sales_flat_order_payment'
       ) }}
       sfop
       ON sfo.entity_id = sfop.parent_id
       LEFT JOIN {{ ref(
              'stg__customer_entity_datetime'
       ) }}
       ced
       ON ced.entity_id = sfo.customer_id
       AND ced.attribute_id = 11 -- AND STR_TO_DATE (ced.value,'%Y-%m-%d') IS NOT NULL
       AND ced.value IS NOT NULL
       LEFT JOIN {{ ref(
              'stg__customer_entity'
       ) }}
       ce
       ON ce.entity_id = sfo.customer_id

WHERE
       sfo.increment_id NOT LIKE '%-%'
       AND (
              sfo.sales_product_type != 12
              OR sfo.sales_product_type IS NULL
       )

