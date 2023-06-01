{{ config(
	materialized='incremental',
	unique_key='id'
) }}

with tickets as (
    select  Id                                                                          as id,
            TicketNumber                                                                as ticket_number,
            Subject                                                                     as subject,
            Status                                                                      as status,
            StatusType                                                                  as status_type,
            CreatedTime                                                                 as created_time,
            DueDate                                                                     as due_date,
            CustomerResponseTime                                                        as customer_response_time,
            ContactId                                                                   as contact_id,
            Channel                                                                     as channel,
            ResponseDueDate                                                             as response_due_date,
            AssigneeId                                                                  as assignee_id,
            ClosedTime                                                                  as closed_time,
            CommentCount                                                                as comment_count,
            if(Channel='Phone', 1, 0)                                                   as phone_ticket,
            if(Channel='Chat', 1, 0)                                                    as chat_ticket,
            if(Channel ='Email', 1, 0)                                                  as email_ticket
        from {{ source(
            'zohodesk',
            'Tickets'
        ) }} 
	where 1=1
 	{% if is_incremental() %}
       and 1=0
       and coalesce(gravity_updated,gravity_inserted) > (select max(coalesce(gravity_updated,gravity_inserted)) from {{this}} )
	{% endif %}       
)

select * from tickets