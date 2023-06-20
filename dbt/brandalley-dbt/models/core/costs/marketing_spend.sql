
{{ config(
    materialized='table'
) }}


select 
    Month             as month_beginning,
    Platform          as platform,
    Cost              as cost,
    Acquisition_Costs as acquisition_costs,
    Retargeting_Costs as retargeting_costs,
    Registrations     as registrations,
    Orders            as orders,
    Revenue           as revenue
from {{ source('analytics', 'marketing_spend_gsheet') }}
where month is not null