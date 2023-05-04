{{ config(
    materialized='table'
)}}

select
    Date           as date,
    AUD            as gbp_to_aud,
    CAD            as gbp_to_cad,
    CHF            as gbp_to_chf,
    EUR            as gbp_to_eur,
    HKD            as gbp_to_hkd,
    USD            as gbp_to_usd,
    EUR_Budget     as gbp_to_eur_budget,
    round(1/coalesce(EUR,EUR_Budget),4) as eur_to_gbp,
    Last_Updated   as updated_at,
from {{ source('analytics', 'fx_rates_gsheet') }}
