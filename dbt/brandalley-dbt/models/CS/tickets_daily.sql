select 
    'Ticket'                            as entity, 
    'Zendesk'                           as source,
    date(created_at)                    as date, 
    id, 
    date(due_at)                        as due_date, 
    status, 
    sum(phone_ticket)                   as phone_ticket, 
    sum(chat_ticket)                    as chat_ticket, 
    sum(email_ticket)                   as email_ticket, 
    null                                as order_count 
from {{ ref('zendesk_tickets_detailed') }}
group by date(created_at), id, date(due_at), status

UNION all

select 
    'Ticket'                            as entity, 
    'Zohodesk'                          as source,
    date(created_time)                  as date, 
    id, 
    date(due_date)                      as due_date, 
    status, 
    sum(phone_ticket)                   as phone_ticket, 
    sum(chat_ticket)                    as chat_ticket, 
    sum(email_ticket)                   as email_ticket, 
    null                                as order_count
from {{ ref('zohodesk_tickets_detailed') }}
group by date(created_time), id, date(due_date), status

UNION all

select 
    'Orders'                            as entity, 
    'Magento'                           as source,
    date(created_at)                    as date, 
    entity_id                           as id, 
    null                                as due_date, 
    status, 
    null                                as phone_ticket, 
    null                                as chat_ticket, 
    null                                as email_ticket, 
    count(distinct entity_id)           as order_count
from {{ ref('Orders') }}
group by date(created_at), entity_id, status
