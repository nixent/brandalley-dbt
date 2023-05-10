select *
    from {{ source(
        'zendesk',
        'call_metric'
    ) }} 