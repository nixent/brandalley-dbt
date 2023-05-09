select *,
IF(time_to_answer<20, 1, 0)                                                                                             as answered_less_than_20s,
FORMAT_DATE('%a', created_at)                                                                                           as created_at_day_of_week,
extract(hour from created_at)                                                                                           as created_at_hour_of_the_day,
FORMAT_DATETIME("%B", DATETIME(created_at))                                                                             as created_at_mon_of_the_year,
case when wait_time < 10 then '0-10 sec'
     when wait_time >=10 and wait_time < 20 then '10-20 sec'
     when wait_time >=20 and wait_time < 30 then '20-30 sec'
     when wait_time >=30 and wait_time < 60 then '30-60 sec'
     when wait_time >=60 and wait_time < 300 then '60-300 sec'
     else '>300 sec' end                                                                                                as wait_time_bucket,
case when duration < 300 then '0-5 min'
     when wait_time >=300 and wait_time < 600 then '5-10 min'
     when wait_time >=600 and wait_time < 900 then '10-15 min'
     when wait_time >=900 and wait_time < 1200 then '15-20 min'
     else '>20 min' end                                                                                                 as call_duration_bucket,
if(completion_status='completed' and direction='Inbound', 1, 0)                                                         as inbound_completed_calls,
if(completion_status='abandoned_in_queue' and direction='Inbound', 1, 0)                                                as inbound_abandoned_in_queue_calls,
if(completion_status='abandoned_in_queue' and direction='Inbound' and time_to_answer<20, 1, 0)                          as inbound_short_abandoned_in_queue_calls,
if(direction='Outbound', 1, 0)                                                                                          as outbound_calls,
ifnull(wait_time, 0) + ifnull(talk_time, 0) + ifnull(time_to_answer, 0) + ifnull(wrap_up_time, 0)                       as aht
    from {{ source(
        'zendesk',
        'call_metric'
    ) }} 