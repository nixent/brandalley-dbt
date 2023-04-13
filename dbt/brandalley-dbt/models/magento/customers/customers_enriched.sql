with customers as (
  select
    cst_id            as customer_id,
    timestamp(dt_cr)  as signed_up_at
  from {{ ref('customers') }}
), 

order_info as (
  select
    customer_id,
    min(created_at)   as first_purchase_at, 
    max(created_at)   as last_purchase_at,
    count(magentoID)  as count_customer_orders
  from {{ ref('Orders') }}
  where customer_id is not null
  group by 1 
),

first_order_brands as (
  select
    customer_id,
    order_id,
    min(created_at)           as order_at, 
    array_agg(distinct brand ignore nulls) as first_purchase_brands
  from {{ ref('OrderLines') }}
  where customer_id is not null
  group by 1,2
  qualify row_number() over (partition by customer_id order by order_at) = 1
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
  c.customer_id,
  c.signed_up_at,
  oi.first_purchase_at,
  oi.last_purchase_at,
  oi.count_customer_orders,
  fob.first_purchase_brands,
  so.second_purchase_at,
  so.first_to_second_order_interval,
  date_diff(current_date, date(oi.first_purchase_at), day) as customer_first_purchase_age_days
from customers c
left join order_info oi
  on c.customer_id = oi.customer_id
left join first_order_brands fob
  on c.customer_id = fob.customer_id
left join second_orders so
  on c.customer_id = so.customer_id
  