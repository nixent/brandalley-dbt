{{ config(
    materialized='table'
)}}

select
    date,
    gbp_to_aud,
    gbp_to_cad,
    gbp_to_chf,
    gbp_to_eur,
    gbp_to_hkd,
    gbp_to_usd,
    gbp_to_eur_budget,
    eur_to_gbp
from {{ ref('stg__fx_rates') }}
