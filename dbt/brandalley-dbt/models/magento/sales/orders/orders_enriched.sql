{{ config(
  materialized='table'
)}}

with order_line_agg as (
  select
    order_id,
    count(*)                             as count_order_lines,
    sum(TOTAL_GBP_ex_tax_after_vouchers) as order_revenue_excl_tax_after_vouchers,
    sum(line_product_cost_exc_vat)       as order_product_costs_excl_tax
  from {{ ref('OrderLines') }}
  group by 1
),

order_refunds_agg as (
  select
    sfc.order_id,
    count(sfc.entity_id)  as count_refunds,
    count(sfci.entity_id) as count_item_refunds,
    sum(row_total)        as total_refund_amount
  from {{ ref('sales_flat_creditmemo') }} sfc
  left join {{ ref('sales_flat_creditmemo_item') }} sfci
    on sfci.parent_id = sfc.entity_id
  group by 1
)

select
  o.magentoID                     as order_id,
  o.increment_id,
  o.customer_id,
  o.created_at                    as order_at,
  o.status                        as order_status,
  o.orderno                       as order_sequence,
  if(o.order_number_incl_cancellations = 1, true, false)  as is_first_order,
	o.order_number_excl_full_refunds,
	o.order_number_incl_cancellations,
  o.interval_between_orders,
  o.days_since_first_purchase,
  o.days_since_signup,
  o.coupon_code,
  if(contains_substr(o.coupon_code, ','), o.coupon_code, sr.name)              as coupon_name,
  src.type                                                                     as coupon_type,
  coalesce(src.coupon_type_label, 'Multiple Codes')                            as coupon_type_label,
  ola.order_revenue_excl_tax_after_vouchers,
  ola.order_product_costs_excl_tax,
  ola.order_revenue_excl_tax_after_vouchers - ola.order_product_costs_excl_tax as order_margin,
  ora.count_refunds,
  ora.count_item_refunds,
  ora.total_refund_amount,
  ola.count_order_lines
from {{ ref('Orders') }} o
left join order_line_agg ola 
  on o.magentoID = ola.order_id
left join order_refunds_agg ora
  on o.magentoID = ora.order_id
left join {{ ref('stg__salesrule_coupon') }} src
  on lower(o.coupon_code) = lower(src.code)
left join {{ ref('stg__salesrule') }} sr
  on src.rule_id = sr.rule_id

