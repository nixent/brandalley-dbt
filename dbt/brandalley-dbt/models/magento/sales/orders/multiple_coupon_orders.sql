with codes as (
    select
        o.customer_id,
        o.ba_site,
        oe.order_id,
        o.increment_id,
        oe.coupon_type_label,
        o.created_at,
        split(o.coupon_code, ',') as code_array
    from {{ ref('Orders') }} o
    left join {{ ref('orders_enriched') }} oe
        on o.increment_id = oe.increment_id and o.ba_site = oe.ba_site
    where contains_substr(oe.coupon_code, ',') 
)

select 
    c.*, 
    code_unnest as coupon_code, 
    sr.name as coupon_name
from codes c,
unnest(code_array) as code_unnest
left join {{ ref('stg__salesrule_coupon') }} src
  on lower(code_unnest) = lower(src.code) and c.ba_site = src.ba_site
left join {{ ref('stg__salesrule') }} sr 
  on src.rule_id = sr.rule_id and src.ba_site = sr.ba_site