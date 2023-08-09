select 
    date(safe_cast(created_at as datetime))                                                                                      as created_at,
    ba_site,
    count (distinct id)                                                                                                          as number_of_calls,
    count (IF(direction='inbound', id, null))                                                                                    as inbound_calls,
    count (IF(direction='outbound', id, null))                                                                                   as outbound_calls,
    count (IF(completion_status='completed', id, null))                                                                          as completed_calls,
    count (IF(completion_status!='completed', id, null))                                                                         as abandoned_calls,
    cast(time(timestamp_seconds(sum(duration))) as string)                                                                       as call_duration,
    cast(time(timestamp_seconds(sum(IF(direction='inbound', duration, 0)))) as string)                                           as inbound_duration,
    cast(time(timestamp_seconds(sum(IF(direction='outbound', duration, 0)))) as string)                                          as outbound_duration,
    cast(time(timestamp_seconds(sum(IF(completion_status='completed', duration, 0)))) as string)                                 as completed_duration,
    cast(time(timestamp_seconds(sum(IF(completion_status!='completed', duration, 0)))) as string)                                as abandoned_duration,
    cast(time(timestamp_seconds(cast(avg(duration) as integer))) as string)                                                      as avg_call_duration,
    cast(time(timestamp_seconds(cast(avg(IF(direction='inbound', duration, null)) as integer))) as string)                       as avg_inbound_duration,
    cast(time(timestamp_seconds(cast(avg(IF(direction='outbound', duration, null)) as integer))) as string)                      as avg_outbound_duration,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status='completed', duration, null)) as integer))) as string)             as avg_completed_duration,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status!='completed', duration, null)) as integer))) as string)            as avg_abandoned_duration,
    cast(time(timestamp_seconds(max(duration))) as string)                                                                       as max_call_duration,
    cast(time(timestamp_seconds(max(IF(direction='inbound', duration, 0)))) as string)                                           as max_inbound_duration,
    cast(time(timestamp_seconds(max(IF(direction='outbound', duration, 0)))) as string)                                          as max_outbound_duration,
    cast(time(timestamp_seconds(max(IF(completion_status='completed', duration, 0)))) as string)                                 as max_completed_duration,
    cast(time(timestamp_seconds(max(IF(completion_status!='completed', duration, 0)))) as string)                                as max_abandoned_duration,
    cast(time(timestamp_seconds(cast(avg(time_to_answer) as integer))) as string)                                                as avg_call_tta,
    cast(time(timestamp_seconds(cast(avg(IF(direction='inbound', time_to_answer, null)) as integer))) as string)                 as avg_inbound_tta,
    cast(time(timestamp_seconds(cast(avg(IF(direction='outbound', time_to_answer, null)) as integer))) as string)                as avg_outbound_tta,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status='completed', time_to_answer, null)) as integer))) as string)       as avg_completed_tta,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status!='completed', time_to_answer, null)) as integer))) as string)      as avg_abandoned_tta,
    cast(time(timestamp_seconds(cast(avg(wait_time) as integer))) as string)                                                     as avg_call_wait_time,
    cast(time(timestamp_seconds(cast(avg(IF(direction='inbound', wait_time, null)) as integer))) as string)                      as avg_inbound_wait_time,
    cast(time(timestamp_seconds(cast(avg(IF(direction='outbound', wait_time, null)) as integer))) as string)                     as avg_outbound_wait_time,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status='completed', wait_time, null)) as integer))) as string)            as avg_completed_wait_time,
    cast(time(timestamp_seconds(cast(avg(IF(completion_status!='completed', wait_time, null)) as integer))) as string)           as avg_abandoned_wait_time
    from {{ source(
        'zendesk',
        'call_metric'
    ) }} 
    {{dbt_utils.group_by(1,2)}}