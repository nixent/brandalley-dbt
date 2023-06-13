{{ config(
	materialized='table',
	unique_key='unique_id',
	cluster_by=['category_name']
) }}

select
    {{dbt_utils.generate_surrogate_key(['p.ba_site_variant_sku', 'ccp.category_id', 'p.ba_site'])}}    as unique_id,
    p.product_id,
    p.variant_product_id,
    p.sku,
    p.variant_sku,
    p.ba_site,
    case
        when cceh.name in ('', 'Women', 'Men', 'Kids', 'Lingerie', 'Home', 'Beauty', 'Z_NoData', 'Archieved outlet products', 'Holding review')
            or cceh.name is null
        then 'Outlet'
        else cceh.name
    end 											as category_name,
    ccp.category_id,
    timestamp(cceh.sale_start)                      as sale_start_at,
    timestamp(cceh.sale_end)                        as sale_end_at,
    row_number() over (partition by ccp.category_id, p.sku) as variant_sku_per_sku_per_sale_number
from {{ ref('products') }} p
left join {{ ref('stg__catalog_category_product') }} ccp
    on p.product_id = ccp.product_id and p.ba_site = ccp.ba_site
left join {{ ref('stg__catalog_category_entity_history') }} cceh
		on ccp.category_id = cceh.category_id and ccp.ba_site = cceh.ba_site
    