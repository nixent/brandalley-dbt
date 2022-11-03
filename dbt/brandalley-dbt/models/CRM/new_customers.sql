SELECT
    `e`.`entity_id`,
    `e`.`entity_type_id`,
    `e`.`attribute_set_id`,
    `e`.`website_id`,
    `e`.`email`,
    `e`.`group_id`,
    `e`.`increment_id`,
    `e`.`store_id`,
    `e`.`created_at`,
    `e`.`updated_at`,
    `e`.`is_active`,
    `e`.`disable_auto_group_change`,
    `at_emarsys_sync_status`.`value` AS `emarsys_sync_status`,
    `at_emarsys_sync_banned`.`value` AS `emarsys_sync_banned`,
    `ns`.`subscriber_id`,
    `ns`.`subscriber_status`,
    `ns`.`subscriber_confirm_code`,
    `ns`.`subscriber_source`,
    `cee`.`line_id`,
    `cee`.`customer_id`,
    `cee`.`number_transactions`,
    `cee`.`ltv`,
    `cee`.`ltvr`,
    `cee`.`first_purchase`,
    `cee`.`last_purchase`,
    `cee`.`first_visit_at`
FROM
    {{ ref('stg__customer_entity') }} AS `e`
    LEFT JOIN {{ ref('stg__customer_entity_int') }} AS `at_emarsys_sync_status`
    ON (
        `at_emarsys_sync_status`.`entity_id` = `e`.`entity_id`
    )
    AND (
        `at_emarsys_sync_status`.`attribute_id` = 278
    )
    LEFT JOIN {{ ref('stg__customer_entity_int') }} AS `at_emarsys_sync_banned`
    ON (
        `at_emarsys_sync_banned`.`entity_id` = `e`.`entity_id`
    )
    AND (
        `at_emarsys_sync_banned`.`attribute_id` = 318
    )
    LEFT JOIN {{ ref('stg__newsletter_subscriber') }} AS `ns`
    ON ns.customer_id = e.entity_id
    AND customer_id != 0
    LEFT JOIN {{ ref('stg__customer_entity_extra') }} AS `cee`
    ON cee.customer_id = e.entity_id
WHERE
    (
        `e`.`entity_type_id` = 1
    )
    AND ((at_emarsys_sync_status.value IN(0, 1))
    OR (at_emarsys_sync_status.value IS NULL))
    AND ((at_emarsys_sync_banned.value = 0)
    OR (at_emarsys_sync_banned.value IS NULL))
    AND (
        `e`.`entity_id` NOT IN (
            165718,
            4706867,
            3690115,
            8610442
        )
    )
ORDER BY
    `e`.`entity_id` DESC
