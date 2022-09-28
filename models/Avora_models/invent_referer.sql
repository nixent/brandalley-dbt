SELECT
    *
FROM
    {{ ref(
        'stg__invent_referer'
    ) }}
