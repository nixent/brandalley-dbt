{{ config(
    schema='emarsys',
    materialized='incremental'
) }}
with ba_customers as (
  select
    customerId,
    email,
    optin,
    _streamkap_loaded_at_ts
  from {{ ref('emarsys_new_customers') }}
  {% if is_incremental() %}
    where
    _streamkap_loaded_at_ts > (SELECT MAX(_streamkap_loaded_at_ts) FROM {{ this }})
  {% endif %}
  qualify row_number() over (partition by customerId order by _streamkap_loaded_at_ts desc) = 1
),
ifg_customers as (
  select
    customer_id as ifg_customer_id,
    email as ifg_email,
    2 as ifg_optin
  from {{ source('emarsys_ifg', 'ifg_*') }}
  where _TABLE_SUFFIX like '%_transformed'
)
select
    *
from ba_customers ba
inner join ifg_customers ifg
on lower(ba.email)=lower(ifg.ifg_email) and ba.optin=1