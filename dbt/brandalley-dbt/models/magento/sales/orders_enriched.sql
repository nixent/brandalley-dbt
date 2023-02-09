WITH customers_orders AS (
  SELECT
    o.customer_id,
    o.magentoID AS order_id,
    MIN(created_at) order_at 
  FROM {{ ref('Orders') }} as o
  GROUP BY  1, 2 )

  , order_rank AS (
  SELECT
    customer_id,
    order_id,
    order_at,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_at) order_sequence
  FROM customers_orders )

  SELECT * FROM order_rank 