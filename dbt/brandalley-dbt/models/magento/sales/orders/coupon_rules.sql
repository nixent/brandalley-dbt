select 
    sc.coupon_id,
    sc.ba_site,
    sc.rule_id,
    sc.code,
    sc.usage_limit,
    sc.usage_per_customer,
    sc.times_used,
    sc.expiration_date,
    sc.is_primary,
    sc.created_at                                                                               as sales_rule_coupon_created_at,
    sc.type, 
    coalesce(sc.coupon_type_label, if(sfo.coupon_rule_name is not null, 'Unknown', 'Multiple Codes')) as coupon_type_label,
    coalesce((sc.times_used >= sc.usage_limit or current_timestamp > sc.expiration_date), false)  as coupon_expired, 
    sfo.increment_id                                                                            as order_id, 
    sfo.status, 
    sfo.base_discount_amount, 
    sfo.base_discount_invoiced, 
    sfo.discount_description, 
    sfo.customer_id,
    timestamp(sfo.created_at)                                                                   as order_date,
    ia.id                                                                                       as invent_autocoupon_id,
    ia.referral_coupon,
    ia.referee_coupon,
    ia.reward_to,
    ia.reward_from,
    ia.created_at                                                                               as invent_autocoupon_created_at, 
    ia.cartrule_id, 
    ia.basis_for_issue,
    case 
        when ia.basis_for_issue = 1 then 'Goodwill'
        when ia.basis_for_issue = 2 then 'Discount'
        when ia.basis_for_issue = 3 then 'Supplier defect'
        when ia.basis_for_issue = 4 then 'Item broken'
        else 'Other'
    end as coupon_reason,
    ia.comments_text,
    ia.recommendation,
    ia.referral_couponid,
    ia.referee_couponid,
    ia.reward_from_old,
    ia.issuer_id,
    sr.discount_amount                                                                          as salesrule_discount_amount
from {{ ref('stg__salesrule_coupon') }} sc
left outer join {{ ref('stg__salesrule') }} sr 
    on sc.rule_id = sr.rule_id and sc.ba_site = sr.ba_site
left outer join {{ ref('stg__sales_flat_order') }} sfo 
    on sc.code = sfo.coupon_code and sc.ba_site = sfo.ba_site
left outer join {{ ref('stg__invent_autocoupon') }} ia 
    on sc.coupon_id = ia.referral_couponid 
        and (ia.reward_from = sfo.customer_id or sfo.customer_id is null) 
        and sc.ba_site = ia.ba_site