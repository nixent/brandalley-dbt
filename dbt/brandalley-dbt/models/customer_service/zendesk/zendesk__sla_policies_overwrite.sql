select 
case when ticket_id like '%UK%' then 'UK' else 'FR' end as ba_site,
*
from
{{ ref('zendesk__sla_policies') }}