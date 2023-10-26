{{ config(
    materialized='table',
    schema='marketing'
)}}

with cj_affiliates as (
select date,
       order_id,
       commission_id,
       posting_date,
       publisher_id,
       publisher_name,
       website_name,
       aid as ad_id,
       action_tracker_id as action_id,
       action_tracker_name as action_name,
       action_status as status,
       cast(sale_amount_adv_currency as numeric) as sales_amount_before_vouchers,
       case when cast(sale_amount_adv_currency as numeric) > 0 then cast(order_discount_adv_currency as numeric) else cast(order_discount_adv_currency as numeric)*-1 end as order_discount,
       cast(adv_commission_amount_adv_currency as numeric) as adv_commission_amount, --need to make sure this is this all gbp
       cast(cj_fee_adv_currency as numeric) as cj_fee_amount,
       case when cast(sale_amount_adv_currency as numeric) > 0 then 'Order' else 'Return' end as transaction_type,
       coupon,
       new_to_file,
       is_cross_device        
from {{ source('cj_affiliates', 'cj_affiliates_commission') }}
)
select *,
       sales_amount_before_vouchers - order_discount as sales_amount_after_vouchers,
       abs(adv_commission_amount + cj_fee_amount) as pub_commission_amount --adv commission is negative whilst cj fee is positive. Always totals to 0 in sheet
from cj_affiliates
