    SELECT 
    STRING_AGG(CAST(category_id AS STRING)) AS parent_child_category_ids,
    e.entity_id AS child_entity_id,
    e.sku AS child_sku,
    stock.min_qty,
    stock.qty,
    IFNULL(parent_relation.parent_id, e.entity_id) AS parent_id,
    parent_entity_relation.sku AS child_parent_sku,
    image.value AS image_value,
    cpvn.value AS name,
    cpevsi.value,
    cpevsiv.sup_id AS suplier_id,
    cpeib.value AS brand_id,
    cpevcoorigin.value AS country_of_manufacture,
    cpedcost.value AS cost,
    cpev_parent_gender.value AS parent_gender,
    cpev_simple_gender.value AS simple_gender,
    cpev_simple_type.value AS simple_product_type,
    cpev_parent_type.value AS parent_product_type,
    cpei_size.value AS size,
    cpei_colour.value AS colour,
    cpedprice.value AS price,
    cpedsprice.value AS special_price,
    cpedoprice.value AS outlet_price,
    cpev_outlet_category.value AS outlet_category,
    parent_relation_child.product_id AS child_id,
    IF(SUM(stock_child.min_qty) < 0,
        'No',
        'Yes') AS canUseForWHSale,
    cpev_barcode.value AS barcode,
    cpev_nego.value AS nego,
    cpn.buyer AS buyer_id,
    CONCAT(au.firstname, ' ', au.lastname) AS buyer
FROM
		{{ ref(
				'stg__catalog_product_entity'
		) }}
		e
        INNER JOIN
		{{ ref(
				'stg__cataloginventory_stock_item'
		) }}
		stock ON stock.product_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_super_link'
		) }}
		parent_relation ON parent_relation.product_id = e.entity_id
        INNER JOIN
		{{ ref(
				'stg__catalog_product_entity'
		) }}
		parent_entity_relation ON parent_entity_relation.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_category_product'
		) }}
		category ON category.product_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		image ON image.attribute_id = 85
        AND image.entity_id = e.entity_id
        INNER JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpvn ON cpvn.attribute_id = 71
        AND cpvn.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpevsi ON cpevsi.attribute_id = 239
        AND cpevsi.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_supplier'
		) }}
		cpevsiv ON cpevsi.value = cpevsiv.supplier_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpeib ON cpeib.attribute_id = 178
        AND cpeib.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpevcoorigin ON cpevcoorigin.attribute_id = 117
        AND cpevcoorigin.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedcost ON cpedcost.attribute_id = 79
        AND cpedcost.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_parent_gender ON cpev_parent_gender.attribute_id = 180
        AND cpev_parent_gender.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_simple_gender ON cpev_simple_gender.attribute_id = 180
        AND cpev_simple_gender.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_simple_type ON cpev_simple_type.attribute_id = 179
        AND cpev_simple_type.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_parent_type ON cpev_parent_type.attribute_id = 179
        AND cpev_parent_type.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpei_size ON cpei_size.attribute_id = 177
        AND cpei_size.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpei_colour ON cpei_colour.attribute_id = 213
        AND cpei_colour.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedprice ON cpedprice.attribute_id = 75
        AND cpedprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedsprice ON cpedsprice.attribute_id = 76
        AND cpedsprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedoprice ON cpedoprice.attribute_id = 224
        AND cpedoprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_outlet_category ON cpev_outlet_category.attribute_id = 205
        AND cpev_outlet_category.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_super_link'
		) }}
		parent_relation_child ON parent_relation_child.parent_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__cataloginventory_stock_item'
		) }}
		stock_child ON stock_child.product_id = parent_relation_child.product_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_barcode ON cpev_barcode.attribute_id = 252
        AND cpev_barcode.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_nego ON cpev_nego.attribute_id = 204
        AND cpev_nego.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_negotiation'
		) }}
		cpn ON cpn.negotiation_id = cpev_nego.value
        LEFT JOIN
        {{ ref(
                'stg__admin_user'
        ) }}
        au ON cpn.buyer = au.user_id
WHERE
    (e.type_id = 'simple')
        AND (stock.qty > 0)
group by 
    e.entity_id,
    e.sku,
    stock.min_qty,
    stock.qty,
    IFNULL(parent_relation.parent_id, e.entity_id),
    parent_entity_relation.sku,
    image.value,
    cpvn.value,
    cpevsi.value,
    cpevsiv.sup_id,
    cpeib.value,
    cpevcoorigin.value,
    cpedcost.value,
    cpev_parent_gender.value,
    cpev_simple_gender.value,
    cpev_simple_type.value,
    cpev_parent_type.value,
    cpei_size.value,
    cpei_colour.value,
    cpedprice.value,
    cpedsprice.value,
    cpedoprice.value,
    cpev_outlet_category.value,
    parent_relation_child.product_id,
    cpev_barcode.value,
    cpev_nego.value,
    cpn.buyer,
    CONCAT(au.firstname, ' ', au.lastname)