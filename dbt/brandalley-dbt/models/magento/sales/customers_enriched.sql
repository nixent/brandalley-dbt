WITH
  customers AS (
  SELECT
    customer_id,
    MIN(created_at) first_purchase_at
  FROM {{ ref('OrderLines') }}
  GROUP BY 1 
  )
  
  SELECT * FROM customers 
  