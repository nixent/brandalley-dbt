with codes as (
    select
        o.customer_id,
        o.order_number,
        oe.coupon_type_label,
        o.created_at,
        split(coupon_code, ',') as code_array
    from {{ ref('OrderLines') }} o
    left join {{ ref('orders_enriched') }} oe
        on o.order_id = oe.order_id
    where o.created_at >= '2023-01-01' 
        and contains_substr(oe.coupon_code, ',')
)

select 
    c.*, 
    code_unnest as code, 
    sr.name as code_name
from codes c,
unnest(code_array) as code_unnest
left join {{ ref('stg__salesrule_coupon') }} src
  on lower(code_unnest) = lower(src.code)
left join {{ ref('stg__salesrule') }} sr
  on src.rule_id = sr.rule_id