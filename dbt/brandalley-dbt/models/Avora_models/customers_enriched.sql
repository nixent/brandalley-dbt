WITH
  customers AS (
  SELECT
    customer_id,
    MIN(created_at) first_purchase_cohort
  FROM {{ ref('OrderLines') }}
  GROUP BY 1 
  )
  
  SELECT * FROM customers 
  