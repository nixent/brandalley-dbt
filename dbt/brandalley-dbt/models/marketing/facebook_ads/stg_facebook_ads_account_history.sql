with account_recent_record as (
    
    select 
        cast(id as {{ dbt.type_bigint() }}) as account_id,
        _fivetran_synced,
        name as account_name,
        account_status,
        business_country_code,
        created_time as created_at,
        currency,
        timezone_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from {{ source(
        'facebook_ads',
        'account_history'
    ) }} 

)

select * 
from account_recent_record