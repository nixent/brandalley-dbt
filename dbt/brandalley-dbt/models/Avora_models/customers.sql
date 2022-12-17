SELECT
       ce.entity_id cst_id,
       '' AS customer_name,
       '' AS email,
       '' AS telephone,
       '' AS billing_street,
       ca_b_26.value billing_city,
       ca_b_30.value billing_postcode,
       ca_b_28.value b_region,
       ca_b_27.value billing_country,
       '' AS shipping_street,
       ca_s_26.value shipping_city,
       ca_s_30.value shipping_postcode,
       ca_s_28.value s_region,
       ca_s_27.value s_country,
       safe_cast(ce.created_at as TIMESTAMP) AS created_at,
       CASE
              ns.subscriber_status
              WHEN 1 THEN 'Opted'
              ELSE 'Not Opted'
       END subscription,
       CASE
              cet_old_acount.value
              WHEN '' THEN NULL
              ELSE cet_old_acount.value
       END old_account_id,
       CASE
              cei_222.value
              WHEN 1 THEN 'Yes'
              ELSE 'No'
       END AS third_party,
       ce.updated_at,
       cei_363.value AS achica_user,
       CASE
              WHEN cei_367.value IS NULL --OR cei_367.value = ''
              THEN TIMESTAMP(
                     ce.created_at
              )
              ELSE cei_367.value
       END AS achica_migration_date
FROM
       {{ ref(
              'stg__customer_entity'
       ) }}
       ce
       LEFT JOIN {{ ref(
              'stg__customer_entity_int'
       ) }}
       cei
       ON ce.entity_id = cei.entity_id
       AND cei.attribute_id = 13
       LEFT JOIN {{ ref(
              'stg__customer_entity_varchar'
       ) }}
       cev_5
       ON ce.entity_id = cev_5.entity_id
       AND cev_5.attribute_id = 5
       LEFT JOIN {{ ref(
              'stg__customer_entity_varchar'
       ) }}
       cev_7
       ON ce.entity_id = cev_7.entity_id
       AND cev_7.attribute_id = 7
       LEFT JOIN {{ ref(
              'stg__customer_entity_text'
       ) }}
       cet_old_acount
       ON ce.entity_id = cet_old_acount.entity_id
       AND cet_old_acount.attribute_id = 217
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_20
       ON cei.value = ca_b_20.entity_id
       AND ca_b_20.attribute_id = 20
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_22
       ON cei.value = ca_b_22.entity_id
       AND ca_b_22.attribute_id = 22
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_24
       ON cei.value = ca_b_24.entity_id
       AND ca_b_24.attribute_id = 24
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_text'
       ) }}
       ca_b_25
       ON cei.value = ca_b_25.entity_id
       AND ca_b_25.attribute_id = 25
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_26
       ON cei.value = ca_b_26.entity_id
       AND ca_b_26.attribute_id = 26
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_27
       ON cei.value = ca_b_27.entity_id
       AND ca_b_27.attribute_id = 27
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_28
       ON cei.value = ca_b_28.entity_id
       AND ca_b_28.attribute_id = 28
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_30
       ON cei.value = ca_b_30.entity_id
       AND ca_b_30.attribute_id = 30
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_31
       ON cei.value = ca_b_31.entity_id
       AND ca_b_31.attribute_id = 31
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_b_32
       ON cei.value = ca_b_32.entity_id
       AND ca_b_32.attribute_id = 32
       LEFT JOIN {{ ref(
              'stg__customer_entity_int'
       ) }}
       cei_s
       ON ce.entity_id = cei_s.entity_id
       AND cei_s.attribute_id = 14
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_20
       ON cei_s.value = ca_s_20.entity_id
       AND ca_s_20.attribute_id = 20
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_22
       ON cei_s.value = ca_s_22.entity_id
       AND ca_s_22.attribute_id = 22
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_24
       ON cei_s.value = ca_s_24.entity_id
       AND ca_s_24.attribute_id = 24
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_text'
       ) }}
       ca_s_25
       ON cei_s.value = ca_s_25.entity_id
       AND ca_s_25.attribute_id = 25
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_26
       ON cei_s.value = ca_s_26.entity_id
       AND ca_s_26.attribute_id = 26
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_27
       ON cei_s.value = ca_s_27.entity_id
       AND ca_s_27.attribute_id = 27
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_28
       ON cei_s.value = ca_s_28.entity_id
       AND ca_s_28.attribute_id = 28
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_30
       ON cei_s.value = ca_s_30.entity_id
       AND ca_s_30.attribute_id = 30
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_31
       ON cei_s.value = ca_s_31.entity_id
       AND ca_s_31.attribute_id = 31
       LEFT JOIN {{ ref(
              'stg__customer_address_entity_varchar'
       ) }}
       ca_s_32
       ON cei_s.value = ca_s_32.entity_id
       AND ca_s_32.attribute_id = 32
       LEFT JOIN {{ ref(
              'stg__newsletter_subscriber'
       ) }}
       ns
       ON ce.entity_id = ns.customer_id
       LEFT JOIN {{ ref(
              'stg__customer_entity_int'
       ) }}
       cei_222
       ON ce.entity_id = cei_222.entity_id
       AND cei_222.attribute_id = 222
       LEFT JOIN {{ ref(
              'stg__customer_entity_int'
       ) }}
       cei_363
       ON cei_363.entity_id = ce.entity_id
       AND cei_363.attribute_id = 363
       AND (
              cei_363.value = 1
              OR cei_363.value = 2
       )
       LEFT JOIN {{ ref(
              'stg__customer_entity_datetime'
       ) }}
       cei_367
       ON cei_367.entity_id = ce.entity_id
       AND cei_367.attribute_id = 367
