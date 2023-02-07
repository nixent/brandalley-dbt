WITH
  customers AS (
  SELECT
    customer_id,
    MIN(created_at) cohort_ts
  FROM {{ ref('OrderLines') }}
  GROUP BY 1 
  )
  
  SELECT * FROM customers 
  