WITH customers_orders AS (
  SELECT
    o.customer_id,
    o.order_id,
    MIN(created_at) order_ts
  FROM `datawarehouse-358408.analytics_magento.OrderLines` as o
  JOIN {{ ref('customers_enriched') }} as c
  ON
    o.customer_id = c.customer_id
    AND c.cohort_ts >= '2021-01-01'
  GROUP BY  1, 2 )

  SELECT * FROM customers_orders 