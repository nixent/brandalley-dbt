SELECT
    *
FROM
    {{ source(
        'streamkap',
        'log_customer'
    ) }}
