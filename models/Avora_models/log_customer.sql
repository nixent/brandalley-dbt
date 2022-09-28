SELECT
    *
FROM
    {{ ref(
        'stg__log_customer'
    ) }}
