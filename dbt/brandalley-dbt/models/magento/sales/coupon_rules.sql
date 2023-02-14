select sc.coupon_id,sc.rule_id,sc.code,sc.usage_limit,sc.usage_per_customer,sc.times_used,sc.expiration_date,sc.is_primary,sc.created_at as sales_rule_coupon_created_at,type, 
sc.times_used >= sc.usage_limit and (sc.expiration_date is null or current_timestamp < expiration_date) as coupon_expired, 
sfo.increment_id as order_id, sfo.status, sfo.base_discount_amount, sfo.base_discount_invoiced, sfo.discount_description, sfo.created_at as order_date,
ia.id invent_autocoupon_id,ia.referral_coupon,ia.referee_coupon,ia.reward_to,ia.reward_from,ia.created_at as invent_autocoupon_created_at, ia.cartrule_id, ia.basis_for_issue,
ia.comments_text,ia.recommendation,ia.referral_couponid,ia.referee_couponid,ia.reward_from_old,ia.issuer_id
 from 
{{ ref(
        'stg__salesrule_coupon'
    ) }} sc
left outer join {{ ref(
        'stg__sales_flat_order'
    ) }} sfo on sc.code=sfo.coupon_code 
left outer join {{ ref(
        'stg__invent_autocoupon'
    ) }} ia on sc.coupon_id=ia.referral_couponid and (ia.reward_from = sfo.customer_id or sfo.customer_id is null) 