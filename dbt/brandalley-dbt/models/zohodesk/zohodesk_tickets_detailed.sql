{{ config(
	materialized='table',
	unique_key='id',
	partition_by = {
      "field": "created_time",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

/* On hold for first run
{% set min_ts = '2023-02-01' %}
{% if execute and is_incremental() %}
  {% set sql %}
    -- Query to see the earliest event date that needs to be rebuilt from for inserted tickets since last run  
    select min(updated_at) as min_ts from (
		select 
			IF(min(gravity_inserted)<min(gravity_updated), min(gravity_inserted), min(gravity_updated)) as updated_at
		from     {{ source(
        'zohodesk',
        'Tickets'
    ) }}
		where updated_at >= ( select IF(max(gravity_inserted)>max(gravity_updated), max(gravity_inserted), max(gravity_updated)) from {{this}} )
	)
  {% endset %}
  {% set result = run_query(sql) %}
  {% set min_ts = result.columns['min_ts'][0]  %}
{% endif %}
*/

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
/*	{% if is_incremental() %}
		and IF(gravity_inserted>gravity_updated or gravity_updated is null, gravity_inserted, gravity_updated) >= '{{min_ts}}'
	{% endif %}*/
)

select * from tickets