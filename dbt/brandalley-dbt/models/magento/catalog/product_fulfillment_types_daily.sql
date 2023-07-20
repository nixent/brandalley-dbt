with daily_order_counts as (
    select
        date(created_at)        as date_day,
        ba_site,
        parent_sku,
        sum(consignment_qty)    as total_consignment_items,
        sum(warehouse_qty)      as total_warehouse_items,
        sum(selffulfill_qty)    as total_selffulfill_items
    from {{ ref('OrderLines') }}
    group by 1,2,3
)

select
    date_day,
    ba_site,
    parent_sku,
    case 
        when greatest(total_consignment_items, total_warehouse_items, total_selffulfill_items) = total_consignment_items then 'Consignment'
        when greatest(total_consignment_items, total_warehouse_items, total_selffulfill_items) = total_selffulfill_items then 'Self Fulfill'
        when greatest(total_consignment_items, total_warehouse_items, total_selffulfill_items) = total_warehouse_items then 'Warehouse'
        else 'Unknown'
    end as shipment_type
from daily_order_counts