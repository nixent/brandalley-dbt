{% test stock_file_qty(model, column_name) %}
with current_stock as (
select
    sum({{column_name}}) as total_sum
from {{ model }}
where ba_site = 'UK'
),
boundaries as (
select
    sum(qty)*1.1 as upper_boundary,
    sum(qty)*0.9 as lower_boundary
from {{ ref('stock_file_daily') }} 
where ba_site= 'UK' and stock_file_date= date_sub(current_date, interval 1 day)
)
select total_sum from current_stock 
cross join boundaries
where not(total_sum between lower_boundary and upper_boundary)
{% endtest %}