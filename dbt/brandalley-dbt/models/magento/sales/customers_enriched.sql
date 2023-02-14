with customers as (
  select
    customer_id,
    min(created_at)   as first_purchase_at, 
    count(magentoID)  as count_orders
  from {{ ref('Orders') }}
  group by 1 
)
  
select * from customers 
  