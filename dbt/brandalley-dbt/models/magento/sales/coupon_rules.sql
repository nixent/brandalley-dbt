select *, times_used >= usage_limit and (expiration_date is null or current_timestamp < expiration_date) as coupon_expired from 
{{ ref(
        'stg__salesrule_coupon'
    ) }}