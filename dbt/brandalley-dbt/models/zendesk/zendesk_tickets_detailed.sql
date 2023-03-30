/* On hold for first run
{{ config(
	materialized='incremental',
	unique_key='id',
	partition_by = {
      "field": "created_at",
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
			min(updated_at) as updated_at
		from     {{ source(
        'zendesk',
        'ticket'
    ) }}
		where updated_at >= ( select max(updated_at) from {{this}} )
	)
  {% endset %}
  {% set result = run_query(sql) %}
  {% set min_ts = result.columns['min_ts'][0]  %}
{% endif %}
*/

with tickets as (
    select  id,
            created_at,
            description,
            due_at,
            is_public,
            merged_ticket_ids,
            status,
            subject,
            updated_at,
            type,
            url,
            via_channel,
            via_source_rel
    from {{ source(
        'zendesk',
        'ticket'
    ) }} 
	where 1=1
	{% if is_incremental() %}
		and updated_at >= '{{min_ts}}'
	{% endif %}
)

select * from tickets