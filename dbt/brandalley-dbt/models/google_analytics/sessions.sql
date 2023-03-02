{{ config(
    materialized='view'
)}}

SELECT count(*) as records, date 
FROM {{ source('76149814', 'ga_sessions_*') }}
 group by 2 order by 2 desc