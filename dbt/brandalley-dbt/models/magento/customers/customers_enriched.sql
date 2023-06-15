with customers as (
  select
    c.cst_id            as customer_id,
    c.email_hash,
    timestamp(c.dt_cr)  as signed_up_at,
    c.ba_site,
    c.achica_user,
    c.achica_migration_date,
    c.cocosa_user,
    c.cocosa_signup_at,
    replace(regexp_replace(ir.source, r"\?.+", ""), '-', '')  as signup_source,
    case 
      when regexp_replace(ir.source, r"\?.+", "") in ('surf-dome', 'country-attire', 'black-leaf', 'derby-house', 'dirt-bike-bitz', 'ride-away', 'simply-scuba', 'webtogs', 'nightgear') then 'referral-ifg'
      else ir.medium 
    end as signup_medium
  from {{ ref('customers') }} c
  left join 
    (
      select entity_id, ba_site, source, medium
      from {{ ref('stg__invent_referer') }}
      where entity_type = 1 
        and event_type = 1
      qualify row_number() over (partition by entity_id, ba_site order by source desc) = 1
    ) ir
    on c.cst_id = ir.entity_id
      and c.ba_site = ir.ba_site
), 

ifg_customers as (
  select 
    md5(lower(email))                        as email_hash, 
    account_creation_date,
    replace(_TABLE_SUFFIX, '_customers', '') as ifg_source
  from {{ source('analytics', 'ifg_*') }}
  qualify row_number() over (partition by email_hash order by account_creation_date) = 1
),

order_info as (
  select
    customer_id,
    ba_site,
    min(created_at)   as first_purchase_at, 
    max(created_at)   as last_purchase_at,
    count(order_id)  as count_customer_orders,
    max(if(created_at < '2023-04-29',created_at,null) as last_purchase_pre_ifg_acquisition
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
),

customers_joined as (
  select 
    c.ba_site || '-' || c.customer_id as ba_site_customer_id,
    c.customer_id,
    c.ba_site,
    c.signed_up_at,
    c.achica_user,
    c.achica_migration_date,
    c.cocosa_user,
    c.cocosa_signup_at,
    if(ifg.email_hash is not null, true, false) as is_existing_ifg_user,
    if(c.signup_medium='referral-ifg' or (date(c.signed_up_at) >= '2023-04-29' and ifg.email_hash is not null), true, false) as is_new_ifg_user,
    if(ifg.email_hash is not null and oi.last_purchase_pre_ifg_acquisition < '2022-04-29', true, false) as is_existing_ifg_elapsed_user,
    coalesce(if(ifg.ifg_source is not null and date(c.signed_up_at) >= '2023-04-29', replace(ifg.ifg_source, '_', ''), null), c.signup_source)                       as signup_source,
    if(ifg.ifg_source is not null and date(c.signed_up_at) >= '2023-04-29', 'referral-ifg', c.signup_medium)                   as signup_medium,
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
  left join ifg_customers ifg
    on c.email_hash = ifg.email_hash
  qualify row_number() over (partition by c.customer_id, c.ba_site order by c.signed_up_at) = 1
)

select
  *,
  case 
    when is_new_ifg_user then 'IFG'
    when achica_user is not null then 'Achica'
    when cocosa_user is not null then 'Cocosa'
    else 'BA'
  end as customer_type
from customers_joined
  