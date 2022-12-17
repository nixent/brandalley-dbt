select 'OrderLines' as entity, DATE(created_at) as date, sku, name as product_name, category_path, product_type, brand, supplier_id, supplier_name, 
colour, gender, size, nego, category_name, department_type, order_status, region, reference, order_number,
cast(null as string) as coupon_rule_name, cast(null as string) as coupon_code, cast(null as string) as method, 
cast(null as string) as shipping_method, customer_id, cast(null as integer) as orderno, 
customer_email as email,
cast(null as integer) as achica_user, cast(null as date) as achica_migration_date,
count(distinct order_item_id) count_orderlines, null as count_orders, null as count_customers,
null as count_customers_orders,
sum(qty_canceled) as qty_canceled, sum(qty_ordered) as qty_ordered, sum(qty_invoiced) as qty_invoiced
, sum(qty_refunded) as qty_refunded, sum(qty_shipped) as qty_shipped, sum(consignment_qty) as consignment_qty, 
sum(warehouse_qty) as warehouse_qty, sum(product_cost_inc_vat) as product_cost_inc_vat, sum(product_cost_exc_vat) as product_cost_exc_vat, 
sum(flash_price_inc_vat) as flash_price_inc_vat, sum(flash_price_exc_vat) as flash_price_exc_vat, 
sum(shipping_refunded) as shipping_refunded, sum(tax_amount) as tax_amount, sum(qty_backordered) as qty_backordered, 
sum(TOTAL_GBP_after_vouchers) as TOTAL_GBP_after_vouchers, cast(null as integer) as total_discount_amount,
cast(null as integer) as shipping_discount_amount, cast(null as integer) as shipping_excl_tax, cast(null as integer) as shipping_incl_tax, 
cast(null as integer) as total_paid, cast(null as integer) as total_refunded, cast(null as integer) as total_due, 
cast(null as integer) as total_invoiced_cost, cast(null as integer) as base_grand_total, null as grand_total,
null as new_orders, null as repeat_orders, null as new_members
from {{ ref('OrderLines') }}
group by DATE(created_at), sku, name, category_path, product_type, brand, supplier_id, supplier_name,
colour, gender, size, nego, category_name, department_type, order_status, region, reference, order_number,
customer_id, customer_email

UNION ALL
select 'Orders' as entity, DATE(created_at) as date, cast(null as string) as sku, cast(null as string) as product_name, 
cast(null as string) as category_path, cast(null as string) as product_type, cast(null as string) as brand, 
cast(null as string) as supplier_id, cast(null as string) as supplier_name, cast(null as string) as colour, 
cast(null as string) as gender, cast(null as string) as size, cast(null as integer) as nego, cast(null as string) as category_name, 
cast(null as string) as department_type, status as order_status, cast(null as string) as region, cast(null as string) as reference, increment_id as order_number, coupon_rule_name, coupon_code, method, shipping_method, customer_id, orderno, email,
cast(null as integer) as achica_user, cast(null as date) as achica_migration_date,
cast(null as integer) as count_orderlines, count(distinct increment_id) as count_orders, cast(null as integer) as count_customers,
count(distinct customer_id) as count_customers_orders,
cast(null as integer) as qty_canceled, cast(null as integer) as qty_ordered, cast(null as integer) as qty_invoiced, 
cast(null as integer) as qty_refunded, cast(null as integer) as qty_shipped, cast(null as integer) as consignment_qty, 
cast(null as integer) as warehouse_qty, cast(null as decimal) as product_cost_inc_vat, cast(null as decimal) as product_cost_exc_vat, 
cast(null as decimal) as flash_price_inc_vat, cast(null as decimal) as flash_price_exc_vat, cast(null as decimal) as shipping_refunded, 
cast(null as decimal) as tax_amount, cast(null as integer) as qty_backordered, cast(null as decimal) as TOTAL_GBP_after_vouchers, 
sum(total_discount_amount) as total_discount_amount,
sum(shipping_discount_amount) as shipping_discount_amount, sum(shipping_excl_tax) as shipping_excl_tax, 
sum(shipping_incl_tax) as shipping_incl_tax, sum(total_paid) as total_paid, sum(total_refunded) as total_refunded,
sum(total_due) as total_due, sum(total_invoiced_cost) as total_invoiced_cost, sum(base_grand_total) as base_grand_total,
sum(grand_total) as grand_total, if(orderno=1, count(distinct magentoID), 0) as new_orders, 
if(orderno>1, count(distinct magentoID), 0) as repeat_orders,
null as new_members
from {{ ref('Orders') }}
group by DATE(created_at), status, increment_id, coupon_rule_name, coupon_code, method, shipping_method, customer_id, 
orderno, email

UNION ALL
select 'Customers' as entity, DATE(dt_cr) as date, cast(null as string) as sku, cast(null as string) as product_name, 
cast(null as string) as category_path, cast(null as string) as product_type, cast(null as string) as brand, 
cast(null as string) as supplier_id, cast(null as string) as supplier_name, cast(null as string) as colour, 
cast(null as string) as gender, cast(null as string) as size, cast(null as integer) as nego, cast(null as string) as category_name, 
cast(null as string) as department_type, cast(null as string) as order_status, cast(null as string) as region, cast(null as string) as reference, cast(null as string) as order_number,
null as coupon_rule_name, null as coupon_code, null as method, null as shipping_method, cst_id as customer_id, 
null as orderno, email,
achica_user, DATE(achica_migration_date) as achica_migration_date,
null as count_orderlines, null as count_orders, count(distinct cst_id) count_customers,
null as count_customers_orders,
null as qty_canceled, null as qty_ordered, null as qty_invoiced, null as qty_refunded, null as qty_shipped, 
null as consignment_qty, null as warehouse_qty, null as product_cost_inc_vat, null as product_cost_exc_vat, 
null as flash_price_inc_vat, null as flash_price_exc_vat, null as shipping_refunded, null as tax_amount, 
null as qty_backordered, null as TOTAL_GBP_after_vouchers, null as total_discount_amount,
null as shipping_discount_amount, null as shipping_excl_tax, null as shipping_incl_tax, null as total_paid, null as total_refunded,
null as total_due, null as total_invoiced_cost, null as base_grand_total, null as grand_total, null as new_orders, null as repeat_orders,
if(achica_user is null OR achica_user != 2, count(distinct cst_id), 0) as new_members
from {{ ref('customers') }}
group by DATE(created_at), cst_id, email, achica_user, DATE(achica_migration_date)