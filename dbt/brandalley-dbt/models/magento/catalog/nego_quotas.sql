{{ config(materialized='table' )}}


with po_orders as (
    select
        ba_site, 
        negotiation_id,
        negotiation_item_id,
        sku,
        sum(to_order) as po_qty
    from {{ ref('purchase_order') }}
    group by 1,2,3,4
),

nego_info as (
    select 
        cpni.negotiation_item_id,
        cpni.negotiation_id,
        cpni.ba_site,
        cpni.updated_at as nego_updated_at,
        cpni.sku    as variant_sku,
        cpni.qty    as nego_qty,
        po.po_qty,
        p.color     as product_color,
        p.size      as product_size,
        p.outlet_category,
        p.brand     as product_brand
    from {{ ref('catalog_product_negotiation_item') }} cpni 
    left join {{ ref('products') }} p
        on cpni.sku = p.variant_sku
        and cpni.ba_site = p.ba_site
        -- this join isnt one to one..
    left join po_orders po
        on po.negotiation_id = cpni.negotiation_id and po.sku = cpni.sku and po.ba_site = cpni.ba_site and po.negotiation_item_id = cpni.negotiation_item_id
),

order_info as (
    select
        ol.sku,
        ol.ba_site,
        round(sum(ol.qty_ordered),2)                                            as qty_sold,
        round(sum(ol.total_local_currency_ex_tax_after_vouchers),2)             as sales_amount,
        round(sum(ol.total_local_currency_after_vouchers),2)                    as gmv,
        round(sum(ol.margin),2)                                                 as margin
    from {{ ref('OrderLines') }} ol
    group by 1,2

)

select 
    {{dbt_utils.generate_surrogate_key(['ni.negotiation_item_id', 'ni.ba_site'])}} as ba_site_negotiation_item_id,
    ni.*,
    oi.gmv,
    oi.sales_amount,
    oi.margin,
    oi.qty_sold
from nego_info ni
left join order_info oi
    on ni.variant_sku = oi.sku 
        and ni.ba_site = oi.ba_site
