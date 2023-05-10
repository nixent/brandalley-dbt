select *,
FORMAT_DATE('%a', created_at)                                       as created_at_day_of_week,
extract(hour from created_at)                                       as created_at_hour_of_the_day,
case when first_reply_time_business_minutes < 60 then '0-1 hrs'
     when first_reply_time_business_minutes <120 then '1-2 hrs'
     when first_reply_time_business_minutes <240 then '2-4 hrs'
     when first_reply_time_business_minutes <480 then '4-8 hrs'
     else '>8 hrs' END                                              as first_reply_time_hours_business_bucket,
case when first_reply_time_business_minutes <=1  then '0-1 mins'
     when first_reply_time_business_minutes <=3 then '1-3 mins'
     when first_reply_time_business_minutes <=10 then '3-10 mins'
     else '>10 mins' END                                            as first_reply_time_min_business_bucket,
case when requester_wait_time_in_business_minutes <=1  then '0-1 mins'
     when requester_wait_time_in_business_minutes <=3 then '1-3 mins'
     when requester_wait_time_in_business_minutes <=10 then '3-10 mins'
     when requester_wait_time_in_business_minutes <=30 then '10-30 mins'
     else '>30 mins' END                                            as requester_wait_time_min_business_bucket,
case when full_resolution_business_minutes < 120 then '0-2 hrs'
     when full_resolution_business_minutes < 240 then '2-4 hrs'
     when full_resolution_business_minutes <480 then '4-8 hrs'
     when full_resolution_business_minutes <960 then '8-16 hrs'
     else '>16 hrs' END                                             as full_resolution_business_bucket,
case when total_agent_replies = 1 then '1'
     when total_agent_replies = 2 then '2'
     when total_agent_replies < 6 then '3-5'
     else '>5' END                                                  as total_agent_replies_bucket,
if(is_one_touch_resolution, 1, 0)                                   as one_touch_tickets,
if(total_agent_replies=0, 1, 0)                                     as zero_reply_ticket,
IF(status in ('closed', 'solved'), 1, 0)                            as solved_ticket,
if(ticket_satisfaction_score='Good', 1, 0)                          as satisified_customer,
if(ticket_satisfaction_score in ('Good', 'Bad'), 1, 0)              as rated_ticket,
if(ticket_satisfaction_score in ('Offered', 'Good', 'Bad'), 1, 0)   as surveyed_ticket,
if(ticket_satisfaction_score in ('Good', 'Bad'), 1, 0)              as rated_ticket_wt_comment
from
{{ ref('zendesk__ticket_metrics') }}