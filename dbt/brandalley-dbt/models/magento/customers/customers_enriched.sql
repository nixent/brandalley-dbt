with customers as (
  select
    customer_id,
    min(created_at)   as first_purchase_at, 
    count(magentoID)  as count_customer_orders
  from {{ ref('Orders') }}
  where customer_id is not null
  group by 1 
),

second_orders as (
  select
    customer_id,
    order_at as second_purchase_at,
    days_since_first_purchase as first_to_second_order_interval 
  from {{ ref('orders_enriched') }}
  where order_sequence = 2
)
  
select 
  c.*,
  second_purchase_at,
  first_to_second_order_interval,
  date_diff(current_date, date(first_purchase_at), day) as customer_first_purchase_age_days
from customers c
left join second_orders so
  on c.customer_id = so.customer_id
  