/* On hold for first run
{{ config(
	materialized='incremental',
	unique_key='id',
	partition_by = {
      "field": "CreatedTime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

{% set min_ts = '2023-02-01' %}
{% if execute and is_incremental() %}
  {% set sql %}
    -- Query to see the earliest event date that needs to be rebuilt from for inserted tickets since last run  
    select min(updated_at) as min_ts from (
		select 
			IF(min(gravity_inserted)<min(gravity_updated), min(gravity_inserted), min(gravity_updated)) as updated_at
		from     {{ source(
        'zohodesk',
        'ticket'
    ) }}
		where updated_at >= ( select IF(max(gravity_inserted)>max(gravity_updated), max(gravity_inserted), max(gravity_updated)) from {{this}} )
	)
  {% endset %}
  {% set result = run_query(sql) %}
  {% set min_ts = result.columns['min_ts'][0]  %}
{% endif %}
*/

with tickets as (
    select  Id,
            TicketNumber,
            Subject,
            Status,
            StatusType,
            CreatedTime,
            DueDate,
            CustomerResponseTime,
            ContactId,
            Channel,
            ResponseDueDate,
            AssigneeId,
            ClosedTime,
            CommentCount
    from {{ source(
        'zohodesk',
        'Tickets'
    ) }} 
	where 1=1
	{% if is_incremental() %}
		and IF(gravity_inserted>gravity_updated or gravity_updated is null, gravity_inserted, gravity_updated) >= '{{min_ts}}'
	{% endif %}
)

select * from tickets