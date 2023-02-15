{{ config(
  materialized='table'
)}}

with order_line_agg as (
  select
    order_id,
    sum(TOTAL_GBP_ex_tax_after_vouchers) as order_revenue_excl_tax_after_vouchers,
    sum(line_product_cost_exc_vat)       as order_product_costs_excl_tax
  from {{ ref('OrderLines') }}
  group by 1
)

select
  o.customer_id,
  o.magentoID   as order_id,
  o.created_at  as order_at,
  o.status      as order_status,
  o.orderno     as order_sequence,
	o.order_number_excl_full_refunds,
	o.order_number_incl_cancellations,
  o.interval_between_orders,
  o.days_since_first_purchase,
  o.days_since_signup,
  ola.order_revenue_excl_tax_after_vouchers,
  ola.order_product_costs_excl_tax,
  ola.order_revenue_excl_tax_after_vouchers - ola.order_product_costs_excl_tax as order_margin
from {{ ref('orders_incremental') }} o
left join order_line_agg ola 
  on o.magentoID = ola.order_id

