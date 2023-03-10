select
    {{dbt_utils.surrogate_key(['p.unique_id', 'ccp.category_id'])}}    as unique_id,
    p.product_id,
    p.variant_product_id,
    p.sku,
    p.variant_sku,
    case
        when cceh.name in ('', 'Women', 'Men', 'Kids', 'Lingerie', 'Home', 'Beauty', 'Z_NoData', 'Archieved outlet products', 'Holding review')
            or cceh.name is null
        then 'Outlet'
        else cceh.name
    end 											as category_name,
    ccp.category_id,
    cceh.name
from {{ ref('products') }} p
left join {{ ref('stg__catalog_category_product') }} ccp
    on p.product_id = ccp.product_id
left join {{ ref('stg__catalog_category_entity_history') }} cceh
		on ccp.category_id = cceh.category_id
    