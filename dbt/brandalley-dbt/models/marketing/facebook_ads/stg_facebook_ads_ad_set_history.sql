with ad_set_recent_record as (
    
    select 
        updated_time as updated_at,
        cast(id as {{ dbt.type_bigint() }}) as ad_set_id,
        name as ad_set_name,
        cast(account_id as {{ dbt.type_bigint() }}) as account_id,
        cast(campaign_id as {{ dbt.type_bigint() }}) as campaign_id,
        start_time as start_at,
        end_time as end_at,
        bid_strategy,
        daily_budget,
        budget_remaining,
        status,
        row_number() over (partition by id order by updated_time desc) = 1 as is_most_recent_record
    from {{ source(
        'facebook_ads',
        'ad_set_history'
    ) }} 

)

select * 
from ad_set_recent_record