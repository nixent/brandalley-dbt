{{ config(materialized="table", tags=["job_daily"]) }}

with
    outbound_kpis as (
        select
            dc.left_warehouse_date as logged_date,
            count(distinct dc.delivery_consignment_id) as shipments_dispatched,
            sum(dc.units_qty) as units_dispatched,
            count(distinct if(dc.service_level = 'Express', dc.delivery_consignment_id, null)) as express_shipments_dispatched,
            sum(if(dc.service_level = 'Express', dc.units_qty, 0)) as express_units_dispatched,
            round(count(distinct if(dc.service_level = 'Express', dc.delivery_consignment_id, null)) / count(distinct dc.delivery_consignment_id),2) as pct_express_shipments,
            round(sum(dc.units_qty) / count(distinct dc.delivery_consignment_id),2) as avg_units_per_shipment,
            round(avg(dc.dispatch_time_days),2) as average_dispatch_time
        from {{ ref("delivery_consignment") }} dc
        where dc.status = 'Shipped'
        group by 1
    ),
    goods_in_kpis as (
        select
            gi.date_arrived as logged_date,
            count(distinct gi.po_id) as goods_in_pos,
            sum(gi.qty_arrived) as goods_in_qty,
            sum(if(gi.stock_type = 'X Dock', qty_arrived, 0)) as x_dock_goods_in_qty,
            round(sum(if(gi.stock_type = 'X Dock', qty_arrived, 0)) / sum(gi.qty_arrived),2) as pct_goods_in_x_dock
        from {{ ref("stg__reactor_goods_in") }} gi
        group by 1
    ),
    returns_kpis as (
        select
            rr.received_at_date as logged_date,
            count(distinct rr.customerid) as returns_processed,
            sum(rr.quantity) as returns_units_processed,
            round(sum(rr.quantity) / count(distinct rr.customerid),2) as units_per_return,
            sum(rr.resaleable_qty) as resaleable_units_processed,
            sum(rr.damaged_qty) as damaged_units_processed
        from {{ ref("return_rans") }} rr
        where rr.completed = 'Returned'
        group by 1
    )
select
    d.date_day,
    ok.shipments_dispatched,
    ok.units_dispatched,
    ok.express_shipments_dispatched,
    ok.express_units_dispatched,
    ok.pct_express_shipments,
    ok.avg_units_per_shipment,
    ok.average_dispatch_time,
    ok1.shipments_dispatched                            as last_weeks_shipments_dispatched,
    ok1.units_dispatched                                as last_weeks_units_dispatched,
    ok1.express_shipments_dispatched                    as last_weeks_express_shipments_dispatched,
    ok1.express_units_dispatched                        as last_weeks_express_units_dispatched,
    ok1.pct_express_shipments                           as last_weeks_pct_express_shipments,
    ok1.avg_units_per_shipment                          as last_weeks_avg_units_per_shipment,
    ok1.average_dispatch_time                           as last_weeks_average_dispatch_time,
    ok2.shipments_dispatched                            as l4_weeks_shipments_dispatched,
    ok2.units_dispatched                                as l4_weeks_units_dispatched,
    ok2.express_shipments_dispatched                    as l4_weeks_express_shipments_dispatched,
    ok2.express_units_dispatched                        as l4_weeks_express_units_dispatched,
    ok2.pct_express_shipments                           as l4_weeks_pct_express_shipments,
    ok2.avg_units_per_shipment                          as l4_weeks_avg_units_per_shipment,
    ok2.average_dispatch_time                           as l4_weeks_average_dispatch_time,
    gik.goods_in_pos,
    gik.goods_in_qty,
    gik.x_dock_goods_in_qty,
    gik.pct_goods_in_x_dock,
    gik1.goods_in_pos                                   as last_weeks_goods_in_pos,
    gik1.goods_in_qty                                   as last_weeks_goods_in_qty,
    gik1.x_dock_goods_in_qty                            as last_weeks_x_dock_goods_in_qty,
    gik1.pct_goods_in_x_dock                            as last_weeks_pct_goods_in_x_dock,
    gik2.goods_in_pos                                   as l4_weeks_goods_in_pos,
    gik2.goods_in_qty                                   as l4_weeks_goods_in_qty,
    gik2.x_dock_goods_in_qty                            as l4_weeks_x_dock_goods_in_qty,
    gik2.pct_goods_in_x_dock                            as l4_weeks_pct_goods_in_x_dock,
    rk.returns_processed,
    rk.returns_units_processed,
    rk.units_per_return,
    rk.resaleable_units_processed,
    rk.damaged_units_processed,
    rk1.returns_processed                               as last_weeks_returns_processed,
    rk1.returns_units_processed                         as last_weeks_returns_units_processed,
    rk1.units_per_return                                as last_weeks_units_per_return,
    rk1.resaleable_units_processed                      as last_weeks_resaleable_units_processed,
    rk1.damaged_units_processed                         as last_weeks_damaged_units_processed,
    rk2.returns_processed                               as l4_weeks_returns_processed,
    rk2.returns_units_processed                         as l4_weeks_returns_units_processed,
    rk2.units_per_return                                as l4_weeks_units_per_return,
    rk2.resaleable_units_processed                      as l4_weeks_resaleable_units_processed,
    rk2.damaged_units_processed                         as l4_weeks_damaged_units_processed
from {{ ref("dates") }} d
left join outbound_kpis ok on d.date_day = ok.logged_date
left join outbound_kpis ok1 on d.date_day = date_add(ok1.logged_date, interval 1 week)
left join outbound_kpis ok2 on d.date_day = date_add(ok2.logged_date, interval 4 week)
left join goods_in_kpis gik on d.date_day = gik.logged_date
left join goods_in_kpis gik1 on d.date_day = date_add(gik1.logged_date, interval 1 week)
left join goods_in_kpis gik2 on d.date_day = date_add(gik2.logged_date, interval 4 week)
left join returns_kpis rk on d.date_day = rk.logged_date
left join returns_kpis rk1 on d.date_day = date_add(rk1.logged_date, interval 1 week)
left join returns_kpis rk2 on d.date_day = date_add(rk2.logged_date, interval 4 week)
where d.up_to_current_date_flag = true