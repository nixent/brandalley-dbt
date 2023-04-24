with stock_file_raw as (
    select 
        e.entity_id                                     as child_entity_id,
        e.sku                                           as child_sku,
        e.ba_site,
        stock.min_qty,
        stock.qty,
        ifnull(parent_relation.parent_id, e.entity_id)  as parent_id,
        parent_entity_relation.sku                      as child_parent_sku,
        if(image.value is not null and image.value!='no_selection', 'https://media.brandalley.co.uk/catalog/product'||image.value,  image.value) as image_value,
        cpvn.value                                      as name,
        cpevsi.value,
        cpevsiv.sup_id                                  as suplier_id,
        cpevsiv.name                                    as supplier_name,
        eaov_brand.value                                as brand,
        cpevcoorigin.value                              as country_of_manufacture,
        cpedcost.value                                  as cost,
        replace(replace(replace(cpev_parent_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') as parent_gender,
        replace(replace(replace(cpev_simple_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') as simple_gender,
        eaov_simple_type.value                          as simple_product_type,
        eaov_parent_type.value                          as parent_product_type,
        eaov_size.value                                 as size,
        eaov_color.value                                as colour,
        cpedprice.value                                 as price,
        cpedsprice.value                                as special_price,
        cpedoprice.value                                as outlet_price,
        cpev_outlet_category.value                      as outlet_category,
        cpev_barcode.value                              as barcode,
        cpev_nego.value                                 as nego,
        cpn.buyer                                       as buyer_id,
        concat(au.firstname, ' ', au.lastname)          as buyer,
        -- pulling level_1>level_2>level_3 from the outlet_category field
        split(cpev_outlet_category.value, '>')[offset(0)] level_1, 
        if(length(cpev_outlet_category.value) - length(regexp_replace(cpev_outlet_category.value, '>', ''))>0, split(cpev_outlet_category.value, '>')[offset(1)], null) level_2, 
        if(length(cpev_outlet_category.value) - length(regexp_replace(cpev_outlet_category.value, '>', ''))>1, split(cpev_outlet_category.value, '>')[offset(2)], null) level_3,
        cpni.tax_rate                                   as tax,
        cpei_tax.value                                  as tax_class,
        if(sum(stock_child.min_qty) < 0, 'No', 'Yes')   as canUseForWHSale,
        min(timestamp(parent_entity_relation.created_at)) as child_parent_sku_created_at,
        string_agg(distinct cast(category_id as string)) as parent_child_category_ids,
        -- Parent category is type 3, flashsale type 1. If there are more than 1 category, we need to put them on the same, but separated with a return carriage character
        if(cpei_menu_type_3.value=3, 
        --stringagg to put multiple lines on one, then 2 replace: 1 for changing separator from comma to return carriage, one to remove the initial 'root catalog>brand alley uk>' of categories
            replace(
                replace(
                    string_agg(distinct category_details.path_name order by path_name)
                , ',', '\n')
            , 'Root Catalog>Brand Alley UK>', '')
        , null) as parent_category,
        -- if(cpei_menu_type_1.value=1, replace(replace(RTRIM(REGEXP_EXTRACT(STRING_AGG(category_details.path_name ORDER BY category_details.created_at), '(?:.*?,){3}'), ','), ',', '\n'), 'Root Catalog>Brand Alley UK>', ''), null) as flashsale_category
        -- STRINGAGG to put multiple lines on one, categories need to be ordered from oldest to newest
        if(cpei_menu_type_1.value=1, string_agg(category_details.path_name order by category_details.created_at), null) as flashsale_category
    from {{ ref('stg__catalog_product_entity') }} e
    inner join {{ ref('stg__cataloginventory_stock_item') }} stock 
        on stock.product_id = e.entity_id
            and e.ba_site = stock.ba_site
    left join (
        select parent_id, product_id, ba_site
        from {{ ref('stg__catalog_product_super_link') }}
        qualify row_number() over (partition by product_id, ba_site order by link_id desc) = 1
        ) parent_relation 
        on parent_relation.product_id = e.entity_id
            and e.ba_site = parent_relation.ba_site
    inner join {{ ref('stg__catalog_product_entity') }} parent_entity_relation 
        on parent_entity_relation.entity_id = parent_relation.parent_id
            and parent_entity_relation.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_category_product') }} category 
        on category.product_id = parent_relation.parent_id
            and category.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} image 
        on image.attribute_id = 85
            and image.entity_id = parent_entity_relation.entity_id
            and image.ba_site = parent_entity_relation.ba_site
    inner join {{ ref('stg__catalog_product_entity_varchar') }} cpvn 
        on cpvn.attribute_id = 71
            and cpvn.entity_id = e.entity_id
            and e.ba_site = cpvn.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpevsi 
        on cpevsi.attribute_id = 239
            and cpevsi.entity_id = e.entity_id
            and e.ba_site = cpevsi.ba_site
    left join {{ ref('stg__catalog_product_supplier') }} cpevsiv 
        on cpevsi.value = cpevsiv.supplier_id
            and cpevsi.ba_site = cpevsiv.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpeib 
        on cpeib.attribute_id = 178
            and cpeib.entity_id = parent_relation.parent_id
            and cpeib.ba_site = parent_relation.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_brand
        on eaov_brand.option_id = cpeib.value
            and eaov_brand.store_id = 0
            and eaov_brand.ba_site = cpeib.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpevcoorigin 
        on cpevcoorigin.attribute_id = 117
            and cpevcoorigin.entity_id = parent_relation.parent_id
            and cpevcoorigin.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cpedcost 
        on cpedcost.attribute_id = 79
            and cpedcost.entity_id = e.entity_id
            and e.ba_site = cpedcost.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_parent_gender 
        on cpev_parent_gender.attribute_id = 180
            and cpev_parent_gender.entity_id = parent_relation.parent_id
            and cpev_parent.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_simple_gender 
        on cpev_simple_gender.attribute_id = 180
            and cpev_simple_gender.entity_id = e.entity_id
            and e.ba_site = cpev_simple.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_simple_type 
        on cpev_simple_type.attribute_id = 179
            and cpev_simple_type.entity_id = e.entity_id
            and e.ba_site = cpev_simple_type.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_simple_type
        on cpev_simple_type.value = cast(eaov_simple_type.option_id as string)
            and eaov_simple_type.store_id = 0
            and cpev_simple.ba_site = eaov_simple.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_parent_type 
        on cpev_parent_type.attribute_id = 179
            and cpev_parent_type.entity_id = parent_relation.parent_id
            and cpev_parent.ba_site = parent_relation.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_parent_type
        on cpev_parent_type.value = cast(eaov_parent_type.option_id as string)
            and eaov_simple_type.store_id = 0
            and cpev_parent_type.ba_site = eaov_parent_type.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_size
        on cpei_size.attribute_id = 177
            and cpei_size.entity_id = e.entity_id
            and e.ba_site = cpei_size.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_size
        on eaov_size.option_id = cpei_size.value
            and eaov_size.store_id = 0
            and eaov_size.ba_site = cpei_size.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_tax 
        on cpei_tax.attribute_id = 122
            and cpei_tax.entity_id = parent_entity_relation.entity_id
            and cpei_tax.ba_site = parent_entity_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_colour 
        on cpei_colour.attribute_id = 213
            and cpei_colour.entity_id = e.entity_id
            and e.ba_site = cpei_colour.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_color
        on eaov_color.option_id = cpei_colour.value
            and eaov_color.store_id = 0
            and eaov_color.ba_site = cpei_colour.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cpedprice 
        on cpedprice.attribute_id = 75
            and cpedprice.entity_id = parent_relation.parent_id
            and cpedprice.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cpedsprice 
        on cpedsprice.attribute_id = 76
            and cpedsprice.entity_id = parent_relation.parent_id
            and cpedsprice.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cpedoprice 
        on cpedoprice.attribute_id = 224
            and cpedoprice.entity_id = parent_relation.parent_id
            and cpedoprice.ba_site = parent_relation.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outlet_category 
        on cpev_outlet_category.attribute_id = 205
            and cpev_outlet_category.entity_id = e.entity_id
            and e.ba_site = cpev_outlet.ba_site
    left join {{ ref('stg__catalog_product_super_link') }} parent_relation_child 
        on parent_relation_child.parent_id = parent_relation.parent_id
            and parent_relation_child.ba_site = parent_relation.ba_site
    left join {{ ref('stg__cataloginventory_stock_item') }} stock_child 
        on stock_child.product_id = parent_relation_child.product_id
            and stock_child.ba_site = parent_relation_child.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_barcode 
        on cpev_barcode.attribute_id = 252
            and cpev_barcode.entity_id = e.entity_id
            and e.ba_site = cpev_barcode.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_nego 
        on cpev_nego.attribute_id = 204
            and cpev_nego.entity_id = e.entity_id
            and e.ba_site = cpev_nego.ba_site
    left join {{ ref('stg__catalog_product_negotiation') }} cpn 
        on cast(cpn.negotiation_id as string) = cpev_nego.value
            and cpn.ba_site = cpev_nego.ba_site
    left join (
        select distinct negotiation_id, parrent_sku, sku, tax_rate, ba_site 
        from {{ ref('stg__catalog_product_negotiation_item') }} ) cpni 
        on cast(cpni.negotiation_id as string) = cpev_nego.value
            and cpni.parrent_sku = parent_entity_relation.sku
            and e.sku = cpni.sku
            and e.ba_site = cpni.ba_site
    left join {{ ref('stg__admin_user') }} au 
        on cpn.buyer = au.user_id
            and cpn.ba_site = au.ba_site
    left join {{ ref('catalog_category_flat_store_1_enriched') }} category_details 
        on category.category_id = category_details.entity_id
            and category.ba_site = category_details.ba_site
    left join {{ ref('stg__catalog_category_entity_int') }} cpei_menu_type_3 
        on cpei_menu_type_3.attribute_id = 373
            and cpei_menu_type_3.entity_id = category.category_id
            and cpei_menu_type_3.value=3
            and cpei_menu_type_3.ba_site = category.ba_site
    left join {{ ref('stg__catalog_category_entity_int') }} cpei_menu_type_1 
        on cpei_menu_type_1.attribute_id = 373
            and cpei_menu_type_1.entity_id = category.category_id
            and cpei_menu_type_1.value=1
            and cpei_menu_type_1.ba_site = category.ba_site
    where e.type_id = 'simple'
        and stock.qty > 0
    {{ dbt_utils.group_by(34) }}, cpei_menu_type_3.value, cpei_menu_type_1.value
 ),

 stock_file_2 as (
    select  
        stock.* except (flashsale_category, child_parent_sku, child_parent_sku_created_at, parent_category, special_price, parent_child_category_ids),
        cat_map.category,
        (select string_agg(distinct value order by value) from unnest(split(flashsale_category, ',')) as value) as flashsale_category,
        string_agg(distinct child_parent_sku)   as child_parent_sku,
        min(child_parent_sku_created_at)        as child_parent_sku_created_at,
        string_agg(distinct parent_category)    as parent_category, 
        min(special_price)                      as special_price
    from stock_file_raw stock
    -- join on the mapping provided by the buying team
    -- the logic is we look at outlet_category first to do the join. If not possible we use parent_category then flashsale_category.	
    left join {{ source('utils', 'category_mapping') }} cat_map 
        on 
            -- join on level 1 (First element of path in outlet_category, 3rd element in parent_category and flashsale_category)
            IF(stock.level_1 is not null, 
                stock.level_1, 
                IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>2, 
                    SPLIT(parent_category, '>')[offset(2)], 
                        if(flashsale_category is not null, 
                            SPLIT(flashsale_category, '>')[offset(2)], null)
                    )
                ) = cat_map.row_label 
        and 
            -- join on level 2 (Second element of path in outlet_category, 4th element in parent_category and flashsale_category)
            IF(stock.level_2 is not null, 
                stock.level_2, 
                IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>3, 
                    SPLIT(parent_category, '>')[offset(3)], 
                        if(LENGTH(flashsale_category) - LENGTH(REGEXP_REPLACE(flashsale_category, '>', ''))>0, 
                            SPLIT(flashsale_category, '>')[offset(3)], null)
                    )
                ) = cat_map.level_2 
        and 
            -- join on level 3 (Third element of path in outlet_category, 5th element in parent_category and flashsale_category)
            IF(stock.level_3 is not null, 
                stock.level_3, 
                IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>4, 
                    SPLIT(parent_category, '>')[offset(4)], 
                        if(LENGTH(flashsale_category) - LENGTH(REGEXP_REPLACE(flashsale_category, '>', ''))>1, 
                            SPLIT(flashsale_category, '>')[offset(4)], null)
                    )
                ) = cat_map.level_3
    {{ dbt_utils.group_by(34) }}, flashsale_category
 )

select  
    * except (flashsale_category, child_parent_sku, child_parent_sku_created_at, parent_category, special_price),
    replace(
        replace(
            if(LENGTH(string_agg(flashsale_category)) - LENGTH(REGEXP_REPLACE(string_agg(flashsale_category), ',', ''))>=3,
                RTRIM(
                    REGEXP_EXTRACT(string_agg(flashsale_category)            
        -- The REGEXP_EXTRACT help to keep only characters up to the 3rd comma as Buying team doesn't want more than 3 categories
                    , '(?:.*?,){3}')
    -- RTRIM to remove the last comma
                , ','),
            string_agg(flashsale_category))
    -- replacing commas by return carriage
        , ',', '\n')
    -- removing the initial unwanted 'Root Catalog>Brand Alley UK>' categories
    , 'Root Catalog>Brand Alley UK>', '')   as flashsale_category, 
    string_agg(distinct child_parent_sku)   as child_parent_sku,
    min(child_parent_sku_created_at)        as child_parent_sku_created_at,
    string_agg(parent_category)             as parent_category, 
    min(special_price)                      as special_price
from stock_file_2
{{ dbt_utils.group_by(33) }}
