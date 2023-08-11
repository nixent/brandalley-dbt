{% test stock_file_qty(model, column_name) %}
select
    sum({{column_name}}) as total_sum
from {{ model }}
having not(total_sum between 300000 and 400000)
{% endtest %}