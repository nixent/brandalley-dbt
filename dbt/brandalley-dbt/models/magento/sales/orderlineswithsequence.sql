with cte as (

    SELECT 
    customer_id
    , order_id 
    , rank() over (partition by customer_id order by order_placed_date) as no_of_orders_since_customer_started_ordering 
    --, RANK() over (partition by customer_id order by MIN(order_placed_date)) as 
    FROM {{ ref('OrderLines') }}
    
)

SELECT *
FROM cte
group by 1, 2, 3
