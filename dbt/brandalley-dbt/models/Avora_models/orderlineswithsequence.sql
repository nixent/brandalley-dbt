with cte as (


    SELECT 
    row_number() over (partition by customer_id order by order_placed_date) as ordering 
    , customer_id
    , order_id 
    , rank() over (partition by customer_id order by order_placed_date) as no_of_orders_since_customer_started_ordering 
    , sum(TOTAL_GBP_ex_tax_after_vouchers) over (partition by customer_id order by customer_id) as total_revenue_amount_per_customer
    FROM {{ ref('OrderLines') }}
    
)

SELECT *
FROM cte

