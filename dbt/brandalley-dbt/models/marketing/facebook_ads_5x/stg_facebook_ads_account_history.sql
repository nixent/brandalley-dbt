{{ config(schema='marketing', materialized='view', tags=["job_daily"]) }}

select
    cast(id as {{ dbt.type_bigint() }}) as account_id,
    _fivetran_synced,
    name as account_name,
    account_status,
    business_country_code,
    created_time as created_at,
    currency,
    timezone_name
from {{ source("facebook_ads_5x", "account_history") }}
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
