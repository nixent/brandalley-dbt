SELECT
    *
FROM
    {{ source(
        'streamkap',
        'invent_referer'
    ) }}
