WITH base_cte as (

    SELECT 
    customer_id
    , DATETIME_DIFF(CURRENT_DATETIME(), max(order_placed_date), DAY)  as days_since_last_order
    , count(order_id) as total_orders_made 
    , round(sum(TOTAL_GBP_ex_tax_after_vouchers) ,0) as total_money_spent
    FROM {{ ref('OrderLines') }}
    WHERE customer_id IS NOT NULL 
    GROUP BY 1 
)  ,

   cte_one as (
    ------------ sales_flat_order staging table has a field that the AVG function can be used on to create average_order_value
        SELECT
        sfo.customer_id
        , CONCAT('£ ', ROUND(AVG(sfo.base_grand_total)),0)  AS  average_order_value
        FROM {{ ref('stg__sales_flat_order') }} as sfo
        GROUP BY 1 
   ) 
   

    , cte_two as (
    ----------------- an additionof the average_time_between_transactions field FROM Orders table

        SELECT 
        b.customer_id as customer_id 
        , days_since_last_order
        , total_orders_made
        , CONCAT('£ ',total_money_spent) as total_money_spent 
        , CONCAT(ROUND(AVG(o.total_interval_between_orders_for_each_customer),0),' days') as average_time_between_transactions
        FROM base_cte as b
        LEFT JOIN {{ ref('Orders')}} as o 
        ON b.customer_id = o.customer_id 
        {{dbt_utils.group_by(4)}}
    
    ) , final as (
    ---------- final cte 
        SELECT 
        two.customer_id 
        , days_since_last_order
        , total_orders_made
        , total_money_spent
        , average_time_between_transactions
        , one.average_order_value
        FROM cte_two as two
        LEFT JOIN cte_one as one
        ON two.customer_id = one.customer_id

    )

    SELECT * FROM final 


    
     

    



--------- unique test on customer_id in yml 
