with customers as (
  select
    c.cst_id            as customer_id,
    timestamp(c.dt_cr)  as signed_up_at,
    c.ba_site,
    c.achica_user,
    c.achica_migration_date,
    regexp_replace(ir.source, r"\?.+", "")  as signup_source,
    case 
      when regexp_replace(ir.source, r"\?.+", "") in ('surf-dome', 'country-attire', 'black-leaf', 'derby-house', 'dirt-bike-bitz', 'ride-away', 'simply-scuba', 'webtogs', 'nightgear') then 'referral-ifg'
      else ir.medium 
    end as signup_medium
  from {{ ref('customers') }} c
  left join {{ ref('stg__invent_referer') }} ir
    on c.cst_id = ir.entity_id
      and c.ba_site = ir.ba_site
      and ir.entity_type = 1 
      and ir.event_type = 1
  qualify row_number() over (partition by customer_id, ba_site order by signup_source desc) = 1
), 

order_info as (
  select
    customer_id,
    ba_site,
    min(created_at)   as first_purchase_at, 
    max(created_at)   as last_purchase_at,
    count(magentoID)  as count_customer_orders
  from {{ ref('Orders') }}
  where customer_id is not null
  group by 1,2
),

first_order_brands as (
  select
    customer_id,
    ba_site,
    order_id,
    min(created_at)           as order_at, 
    array_agg(distinct brand ignore nulls) as first_purchase_brands
  from {{ ref('OrderLines') }}
  where customer_id is not null
  group by 1,2,3
  qualify row_number() over (partition by customer_id, ba_site order by order_at) = 1
),

second_orders as (
  select
    customer_id,
    ba_site,
    order_at as second_purchase_at,
    days_since_first_purchase as first_to_second_order_interval 
  from {{ ref('orders_enriched') }}
  where order_sequence = 2
)
  
select 
  c.ba_site || '-' || c.customer_id as ba_site_customer_id,
  c.customer_id,
  c.ba_site,
  c.signed_up_at,
  c.achica_user,
  c.achica_migration_date,
  c.signup_source,
  c.signup_medium,
  oi.first_purchase_at,
  oi.last_purchase_at,
  oi.count_customer_orders,
  fob.first_purchase_brands,
  so.second_purchase_at,
  so.first_to_second_order_interval,
  date_diff(current_date, date(oi.first_purchase_at), day) as customer_first_purchase_age_days
from customers c
left join order_info oi
  on c.customer_id = oi.customer_id and c.ba_site = oi.ba_site
left join first_order_brands fob
  on c.customer_id = fob.customer_id and c.ba_site = fob.ba_site
left join second_orders so
  on c.customer_id = so.customer_id and c.ba_site = so.ba_site
  