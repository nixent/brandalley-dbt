{{ config(
  materialized='table'
)}}

with order_line_agg as (
  select
    order_id,
    ba_site,
    count(*)                             as count_order_lines,
    -- Adding a step to get the max number of suppliers per order
    count(distinct shipment_type)        as max_shipment_type,
    sum(total_local_currency_ex_tax_after_vouchers) as order_revenue_excl_tax_after_vouchers,
    sum(line_product_cost_exc_vat)       as order_product_costs_excl_tax,
    sum(qty_ordered)                     as order_qty_ordered
  from {{ ref('OrderLines') }}
  group by 1,2
),

order_refunds_agg as (
  select
    sfc.order_id,
    sfc.ba_site,
    count(sfc.entity_id)                 as count_refunds,
    count(sfci.entity_id)                as count_item_refunds,
    sum(row_total)                       as total_refund_amount,
  from {{ ref('sales_flat_creditmemo') }} sfc
  left join {{ ref('sales_flat_creditmemo_item') }} sfci
    on sfci.parent_id = sfc.entity_id and sfc.ba_site = sfci.ba_site
  group by 1,2
)

select
  o.ba_site || '-' || o.increment_id as ba_site_increment_id,
  o.order_id,
  o.increment_id,
  o.customer_id,
  o.ba_site,
  o.created_at                    as order_at,
  o.status                        as order_status,
  o.orderno                       as order_sequence,
  if(o.order_number_incl_cancellations = 1, true, false)  as is_first_order,
  if(o.order_number_incl_cancellations = 1, 'New Customer', 'Repeat Customer')  as new_customer,
	o.order_number_excl_full_refunds,
	o.order_number_incl_cancellations,
  o.interval_between_orders,
  o.days_since_first_purchase,
  o.days_since_signup,
  o.coupon_code,
  coalesce(sr.name, o.coupon_rule_name, o.coupon_code)                                                                 as coupon_name,
  src.type                                                                                                             as coupon_type,
  coalesce(src.coupon_type_label, if(o.coupon_rule_name is not null, 'Unknown', 'Multiple Codes'))                     as coupon_type_label,
  ola.order_revenue_excl_tax_after_vouchers,
  ola.order_product_costs_excl_tax,
  ola.order_revenue_excl_tax_after_vouchers - ola.order_product_costs_excl_tax as order_margin,
  ora.count_refunds,
  ora.count_item_refunds,
  ora.total_refund_amount,
  ola.count_order_lines,
  case 
    when lead(o.created_at) over (partition by o.customer_id order by o.created_at) is not null then true 
    else false 
  end as has_ordered_since,
  ola.order_qty_ordered,
  ola.max_shipment_type
from {{ ref('Orders') }} o
left join order_line_agg ola 
  on o.order_id = ola.order_id and o.ba_site = ola.ba_site
left join order_refunds_agg ora
  on o.order_id = ora.order_id and o.ba_site = ora.ba_site
left join {{ ref('stg__salesrule_coupon') }} src
  on lower(o.coupon_code) = lower(src.code) and o.ba_site = src.ba_site
left join {{ ref('stg__salesrule') }} sr
  on src.rule_id = sr.rule_id and sr.ba_site = src.ba_site