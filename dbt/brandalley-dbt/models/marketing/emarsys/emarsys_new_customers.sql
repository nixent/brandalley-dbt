{{ config(
    schema='emarsys',
    materialized='incremental',
    partition_by = {
      "field": "_streamkap_loaded_at_ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    pre_hook="
        {% if is_incremental() %}
            update {{ this }} set emarsys_sync_status = 2 WHERE emarsys_sync_status IN (0 , 1) OR emarsys_sync_status IS NULL;
        {% endif %}"
) }}
SELECT * FROM
(WITH country_map AS(SELECT *
  FROM
    UNNEST(
        [
        STRUCT('AU' AS country,'9' AS country_code), 
        STRUCT('CN' AS country,'37' AS country_code), 
        STRUCT('FR' AS country,'61' AS country_code),
        STRUCT('DE' AS country,'65' AS country_code),
        STRUCT('IE' AS country,'81' AS country_code),
        STRUCT('GB' AS country,'184' AS country_code),
        STRUCT('GG' AS country,'184' AS country_code),
        STRUCT('JE' AS country,'184' AS country_code)
        ]
    )),
CUSTOMERS AS (SELECT * 
    FROM
        {{ ref('stg_uk__customer_entity') }} AS e WHERE e.entity_id NOT IN (select user_id from {{ source('analytics', 'ifg_*') }}) and e.entity_type_id = 1 
        {% if is_incremental() %}
            and  
            e._streamkap_loaded_at_ts > (SELECT MAX(_streamkap_loaded_at_ts) FROM {{ this }})
        {% else %}
            and
            DATE(e._streamkap_loaded_at_ts) >= (SELECT CURRENT_DATE())
        {% endif %})
SELECT 
    `e`.`entity_id` as `hightouch_customerId`,
    `e`.`entity_id` as `customerId`,
    `e`.`email`,
    `cev_fn`.`value` as `firstName`,
    `cev_ln`.`value` as `lastName`,
    `eaov_bp`.`value` as `baTitle`,
    DATE(`e`.`created_at`) as `createdAccountDate`,
    DATE(`ced_dob`.`value`) as `dob`,
    CAST(`cei_g`.`value` AS INT) AS `gender`,
    COALESCE(`cei_ope`.`value`, 0) as `rdpEmails`,
    COALESCE(`cei_de`.`value`, 0) as `dailyEmails`,
    COALESCE(`cei_we`.`value`, 0) as `weeklyEmails`,
    IF(`ns`.`subscriber_status`=1,1, 2) as `optin`,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(`cee`.`first_purchase`)) as `firstPurchaseDate`,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(`cee`.`last_purchase`)) as `lastPurchaseDate`,
    `cee`.`number_transactions` as `numberTransactions`,
    CAST(`cee`.`ltv` AS STRING) AS `ltv`,
    CAST(`cee`.`ltvr` AS STRING) AS `ltvr`,
    `cev_as`.`value` as `autosignin`,
    `ns`.`subscriber_confirm_code` as `subscriberConfirmCode`,
    `ns`.`subscriber_id` as `subscriberId`,
    `ns`.`subscriber_source` as `subscriberSource`,
    `cev_ip`.`value` as `ipAddress`,
    `cev_ipu`.`value` as  `updatedDate`,
    `cei_as`.`value` as `achicaDataSource`,
    `cei_rs`.`value`  as `recordSource`,
    `cei_ess`.`value` as `emarsys_sync_status`,
    `cei_esb`.`value` as `emarsys_sync_banned`,
    split(`caet_street`.`value`,"\n")[offset(0)] as `street`,
    `caev_city`.`value` as `city`,
    `caev_zip`.`value` as `postcode`,
    `cm`.`country_code` as `country`,
    `caev_phone`.`value` as `telephone`,
    `caev_company`.`value` as `company`,
    `e`.`_streamkap_loaded_at_ts` as `_streamkap_loaded_at_ts`,
    ROW_NUMBER() OVER(PARTITION BY `e`.`entity_id` ORDER BY `e`.`_streamkap_loaded_at_ts` DESC) AS id_num
FROM
        CUSTOMERS AS `e`
    INNER JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_ess` ON cei_ess.entity_id = e.entity_id AND cei_ess.attribute_id = 278 AND ((cei_ess.value IN (0 , 1, 2)) OR (cei_ess.value IS NULL)) AND (e.entity_type_id = 1)
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_esb` ON cei_esb.entity_id = e.entity_id AND cei_esb.attribute_id = 318 AND ((cei_esb.value = 0) OR (cei_esb.value IS NULL))
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_varchar') }} AS `cev_fn` ON cev_fn.entity_id = e.entity_id AND cev_fn.attribute_id = 5 
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_varchar') }} AS `cev_ln` ON cev_ln.entity_id = e.entity_id AND cev_ln.attribute_id = 7
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_bp` ON cei_bp.entity_id = e.entity_id AND cei_bp.attribute_id = 202
    LEFT JOIN 
        {{ ref('stg_uk__eav_attribute_option_value') }} `eaov_bp` ON eaov_bp.option_id = cei_bp.value
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_datetime') }} AS `ced_dob` ON ced_dob.entity_id = e.entity_id AND ced_dob.attribute_id = 11
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_g` ON cei_g.entity_id = e.entity_id AND cei_g.attribute_id = 18
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_ope` ON cei_ope.entity_id = e.entity_id AND cei_ope.attribute_id = 222
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_de` ON cei_de.entity_id = e.entity_id AND cei_de.attribute_id = 219
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_we` ON cei_we.entity_id = e.entity_id AND cei_we.attribute_id = 221
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_varchar') }} AS `cev_as` ON cev_as.entity_id = e.entity_id AND cev_as.attribute_id = 235
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_varchar') }} AS `cev_ip` ON cev_ip.entity_id = e.entity_id AND cev_ip.attribute_id = 328
    LEFT JOIN 
        {{ ref('stg_uk__customer_entity_varchar') }} AS `cev_ipu` ON cev_ipu.entity_id = e.entity_id AND cev_ipu.attribute_id = 329
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_as` ON cei_as.entity_id = e.entity_id AND cei_as.attribute_id = 363
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_rs` ON cei_rs.entity_id = e.entity_id AND cei_rs.attribute_id = 381
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_int') }} AS `cei_db` ON cei_db.entity_id = e.entity_id AND cei_db.attribute_id = 13
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_text') }} AS `caet_street` ON caet_street.entity_id = cei_db.value and caet_street.attribute_id = 25
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_varchar') }} AS `caev_city` ON caev_city.entity_id = cei_db.value and caev_city.attribute_id = 26
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_varchar') }} AS `caev_zip` ON caev_zip.entity_id = cei_db.value and caev_zip.attribute_id = 30
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_varchar') }} AS `caev_country` ON caev_country.entity_id = cei_db.value and caev_country.attribute_id = 27
    LEFT JOIN 
        country_map cm ON caev_country.value = cm.country
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_varchar') }} AS `caev_phone` ON caev_phone.entity_id = cei_db.value and caev_phone.attribute_id = 31
    LEFT JOIN
        {{ ref('stg_uk__customer_address_entity_varchar') }} AS `caev_company` ON caev_company.entity_id = cei_db.value and caev_company.attribute_id = 24
    LEFT JOIN
        {{ ref('stg_uk__newsletter_subscriber') }} AS `ns` ON ns.customer_id = e.entity_id AND customer_id != 0
    LEFT JOIN
        {{ ref('stg_uk__customer_entity_extra') }} AS `cee` ON cee.customer_id = e.entity_id
    )
    WHERE id_num=1