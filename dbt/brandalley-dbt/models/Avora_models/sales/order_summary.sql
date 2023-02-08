
with cte_one as (

    SELECT  
    FORMAT_TIMESTAMP('%Y%m%d', MAX(CAST(sfo.created_at AS TIMESTAMP))) as serial,
    IF( FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(cast(sfo.created_at as timestamp)), 'Europe/London') =  FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(cast(sfo.created_at as timestamp)), 'Europe/London'), FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(cast(sfo.created_at as timestamp)), 'Europe/London'), NULL
    ) AS period ,
    COUNT(sfo.entity_id) AS total_orders,
    CONCAT('£', FORMAT("%'.2f", SUM(sfo.base_grand_total-sfo.tax_amount)))  AS grand_total,
    CONCAT('£', FORMAT("%'.2f", AVG(sfo.base_grand_total)))  AS  aov
    FROM {{ ref('stg__sales_flat_order') }} as sfo
    WHERE sfo.entity_id > 7331688 AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) <= TIMESTAMP(sfo.created_at)
    GROUP BY 2

), cte_two as (
    SELECT  
    FORMAT_TIMESTAMP('%Y%m%d', MAX(CAST(sfo.created_at AS TIMESTAMP))) as serial,
    IF(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY ) = MAX(CAST(sfo.created_at AS TIMESTAMP)), 'Yesterday', NULL
    ) AS period ,
    COUNT(sfo.entity_id) AS total_orders,
    CONCAT('£', FORMAT("%'.2f", SUM(sfo.base_grand_total-sfo.tax_amount)))  AS grand_total,
    CONCAT('£', FORMAT("%'.2f", AVG(sfo.base_grand_total)))  AS  aov
    FROM {{ ref('stg__sales_flat_order') }} as sfo
    WHERE sfo.entity_id > 7331688 AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) <= TIMESTAMP(sfo.created_at)

), cte_three as (
    SELECT  
    FORMAT_TIMESTAMP('%Y%m%d', MAX(CAST(sfo.created_at AS TIMESTAMP))) as serial,
    IF(CURRENT_TIMESTAMP() = MAX(CAST(sfo.created_at AS TIMESTAMP)), 'Today', NULL) AS period,
    COUNT(sfo.entity_id) AS total_orders,
    CONCAT('£', FORMAT("%'.2f", SUM(sfo.base_grand_total-sfo.tax_amount)))  AS grand_total,
    CONCAT('£', FORMAT("%'.2f", AVG(sfo.base_grand_total)))  AS  aov
    FROM {{ ref('stg__sales_flat_order') }} as sfo
    WHERE sfo.entity_id > 7331688 AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) <= TIMESTAMP(sfo.created_at)
    --GROUP BY FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(sfo.created_at), 'Europe/London')
    --, FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(sfo.created_at), 'Europe/London') 

)  , cte_four as (
    SELECT
    CAST(50501231 AS STRING) as serial,
    'Last Hour' AS period,    
    COUNT(sfo_today.entity_id) AS total_orders,
    CONCAT('£', FORMAT("%'.2f", SUM(sfo_today.base_grand_total-sfo_today.tax_amount)))  AS grand_total,
    CONCAT('£', FORMAT("%'.2f", AVG(sfo_today.base_grand_total)))  AS aov
    FROM {{ ref('stg__sales_flat_order') }} as sfo_today
    WHERE sfo_today.entity_id > 7331688 AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) <=  TIMESTAMP(sfo_today.created_at)

) , final as (
    SELECT serial, period, total_orders, grand_total, aov FROM cte_one AS one
    UNION DISTINCT
    SELECT serial, period, total_orders, grand_total, aov FROM cte_two AS two
    UNION DISTINCT
    SELECT serial, period, total_orders, grand_total, aov FROM cte_three AS three
    UNION DISTINCT
    SELECT serial, period, total_orders, grand_total, aov FROM cte_four AS four
    ORDER BY serial DESC
)
SELECT  
period as Period,
total_orders as Total_Orders,
grand_total as Grand_Total,
aov as AOV
FROM final
WHERE period IS NOT NULL 




