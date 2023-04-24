select 
    date(safe_cast(created_at as datetime))                                                                as created_at,
    count (distinct id)                                                                                    as number_of_calls,
    count (IF(direction='inbound', id, null))                                                              as inbound_calls,
    count (IF(direction='outbound', id, null))                                                             as outbound_calls,
    count (IF(completion_status='completed', id, null))                                                    as completed_calls,
    count (IF(completion_status!='completed', id, null))                                                   as abandoned_calls,
    time(timestamp_seconds(sum(duration)))                                                                 as call_duration,
    time(timestamp_seconds(sum(IF(direction='inbound', duration, 0))))                                     as inbound_duration,
    time(timestamp_seconds(sum(IF(direction='outbound', duration, 0))))                                    as outbound_duration,
    time(timestamp_seconds(sum(IF(completion_status='completed', duration, 0))))                           as completed_duration,
    time(timestamp_seconds(sum(IF(completion_status!='completed', duration, 0))))                          as abandoned_duration,
    time(timestamp_seconds(cast(avg(duration) as integer)))                                                as avg_call_duration,
    time(timestamp_seconds(cast(avg(IF(direction='inbound', duration, null)) as integer)))                 as avg_inbound_duration,
    time(timestamp_seconds(cast(avg(IF(direction='outbound', duration, null)) as integer)))                as avg_outbound_duration,
    time(timestamp_seconds(cast(avg(IF(completion_status='completed', duration, null)) as integer)))       as avg_completed_duration,
    time(timestamp_seconds(cast(avg(IF(completion_status!='completed', duration, null)) as integer)))      as avg_abandoned_duration,
    time(timestamp_seconds(max(duration)))                                                                 as max_call_duration,
    time(timestamp_seconds(max(IF(direction='inbound', duration, 0))))                                     as max_inbound_duration,
    time(timestamp_seconds(max(IF(direction='outbound', duration, 0))))                                    as max_outbound_duration,
    time(timestamp_seconds(max(IF(completion_status='completed', duration, 0))))                           as max_completed_duration,
    time(timestamp_seconds(max(IF(completion_status!='completed', duration, 0))))                          as max_abandoned_duration
    from {{ source(
        'zendesk',
        'call_metric'
    ) }} 
    {{dbt_utils.group_by(1)}}