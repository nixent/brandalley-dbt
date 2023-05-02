select *
from
{{ ref('zendesk__tickets_metrics') }}