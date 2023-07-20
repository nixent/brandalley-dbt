select
    updated_time as updated_at,
    cast(id as {{ dbt.type_bigint() }}) as ad_id,
    name as ad_name,
    cast(account_id as {{ dbt.type_bigint() }}) as account_id,
    cast(ad_set_id as {{ dbt.type_bigint() }}) as ad_set_id,
    cast(campaign_id as {{ dbt.type_bigint() }}) as campaign_id,
    cast(creative_id as {{ dbt.type_bigint() }}) as creative_id
from {{ source("facebook_ads_5x", "ad_history") }}
qualify row_number() over (partition by id order by updated_time desc) = 1
