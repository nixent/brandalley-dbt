select *
    from {{ source(
        'zendesk',
        'call_leg'
    ) }} 