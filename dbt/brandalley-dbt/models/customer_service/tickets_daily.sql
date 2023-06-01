select 
    'Ticket'                            as entity, 
    'Zendesk'                           as source,
    date(datetime(created_at, "Europe/London"))                    as date, 
    date(datetime(due_at, "Europe/London"))                        as due_date, 
    status, 
    sum(phone_ticket)                   as phone_ticket, 
    sum(chat_ticket)                    as chat_ticket, 
    sum(web_email_ticket)               as email_ticket, 
    null                                as order_count 
from {{ ref('zendesk_tickets_detailed') }}
where created_at >= '2023-03-27 17:45:00'
group by 1,2,3,4,5,9

UNION all

select 
    'Ticket'                            as entity, 
    'Zohodesk'                          as source,
    date(datetime(created_time, "Europe/London"))                  as date, 
    date(datetime(due_date, "Europe/London"))                      as due_date, 
    status, 
    sum(phone_ticket)                   as phone_ticket, 
    sum(chat_ticket)                    as chat_ticket, 
    sum(email_ticket)                   as email_ticket, 
    null                                as order_count
from {{ ref('zohodesk_tickets_detailed')}}
where created_time < '2023-03-27 17:45:00'
group by 1,2,3,4,5,9

UNION all

select 
    'Orders'                            as entity, 
    'Magento'                           as source,
    date(datetime(created_at, "Europe/London"))                    as date, 
    date(null)                                as due_date, 
    status, 
    null                                as phone_ticket, 
    null                                as chat_ticket, 
    null                                as email_ticket, 
    count(distinct magentoID)           as order_count
from {{ ref('Orders') }}
group by 1,2,3,4,5,6,7,8
