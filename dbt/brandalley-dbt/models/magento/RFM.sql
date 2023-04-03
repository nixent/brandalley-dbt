{{ config(enabled=false) }}

with base_cte as (
    select 
        customer_id, 
        datetime_diff(current_datetime(), max(order_placed_date), day)  as days_since_last_order,
        count(distinct order_id)                                        as total_orders_made, 
        round(sum(TOTAL_GBP_ex_tax_after_vouchers),0)                   as total_money_spent
    from {{ ref('OrderLines') }}
    where customer_id is not null 
    group by 1 
),

cte_two as (
----------------- an additionof the average_time_between_transactions field FROM Orders table
    select 
        b.customer_id, 
        days_since_last_order, 
        total_orders_made, 
        total_money_spent, 
        concat(round(avg(o.interval_between_orders),0),' days') as average_time_between_transactions
    from base_cte as b
    left join {{ ref('Orders')}} as o 
        on b.customer_id = o.customer_id 
    {{dbt_utils.group_by(4)}}
) 

select 
    two.customer_id, 
    days_since_last_order, 
    total_orders_made, 
    total_money_spent, 
    average_time_between_transactions, 
    round(total_money_spent/total_orders_made,2) as average_order_value
from cte_two as two


    
     

    



--------- unique test on customer_id in yml 
