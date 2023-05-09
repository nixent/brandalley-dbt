select *,
sla_elapsed_time-target                             as breached_time
from{{ ref('zendesk__sla_policies') }}