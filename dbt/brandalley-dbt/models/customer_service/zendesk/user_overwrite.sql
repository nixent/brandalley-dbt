select *
    from {{ source(
        'zendesk',
        'user'
    ) }} 