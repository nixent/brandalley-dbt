/*with cte as (

    SELECT 
    COUNT(DISTINCT customer_id) as customers
    , order_id  
    , MIN(order_placed_date) AS order_ts 
    --, rank() over (partition by customer_id order by order_placed_date) as no_of_orders_since_customer_started_ordering 
    FROM {{ ref('OrderLines') }}
    GROUP BY 2
)  
, cte_two as (

    SELECT 
    customers 
    , order_id
    , order_ts 
    , RANK() OVER (PARTITION BY customers  ORDER BY order_ts ) AS order_sequence 
    FROM cte 

)
    SELECT * FROM cte_two 
*/
WITH
  customers AS (
  SELECT
    customer_id,
    MIN(created_at) cohort_ts
  FROM
    `datawarehouse-358408.analytics_magento.OrderLines`
  GROUP BY
    1 ),
  customers_orders AS (
  SELECT
    o.customer_id,
    o.order_id,
    MIN(created_at) order_ts
  FROM
    `datawarehouse-358408.analytics_magento.OrderLines` o
  JOIN
    customers c
  ON
    o.customer_id = c.customer_id
    AND c.cohort_ts >= '2021-01-01'
  GROUP BY
    1,
    2 ),
  order_rank AS (
  SELECT
    customer_id,
    order_id,
    order_ts,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_ts) order_sequence
  FROM
    customers_orders ),
  order_sequence_groups AS (
  SELECT
    order_sequence,
    COUNT(DISTINCT customer_id) customers
  FROM
    order_rank
  GROUP BY
    1
  ORDER BY
    1 )
    SELECT order_sequence FROM order_sequence_groups 

