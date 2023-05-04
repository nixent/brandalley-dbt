{{ config(
    materialized='table'
)}}

with daily_dates as (
    select
        date_day
    from unnest(generate_date_array('2020-12-01', current_date, interval 1 day)) as date_day
)

select
    dd.date_day,
    fx.date as fx_date_month,
    fx.updated_at,
    fx.gbp_to_aud,
    fx.gbp_to_cad,
    fx.gbp_to_chf,
    fx.gbp_to_eur,
    fx.gbp_to_hkd,
    fx.gbp_to_usd,
    fx.gbp_to_eur_budget,
    fx.eur_to_gbp
from {{ ref('stg__fx_rates') }} fx
left join daily_dates dd 
    on fx.date = date_trunc(dd.date_day, month)
