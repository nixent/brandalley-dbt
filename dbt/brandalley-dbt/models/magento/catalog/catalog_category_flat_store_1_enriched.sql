select main.*,
concat(lv0.name,IF (lv1.name IS NOT NULL,concat('>',lv1.name),''),IF (lv2.name IS NOT NULL,concat('>',lv2.name),''), IF (lv3.name IS NOT NULL,concat('>',lv3.name),''), IF (lv4.name IS NOT NULL,concat('>',lv4.name),''), IF (lv5.name IS NOT NULL,concat('>',lv5.name),''), IF (lv6.name IS NOT NULL,concat('>',lv6.name),''), IF (lv7.name IS NOT NULL,concat('>',lv7.name),'')) path_name
FROM {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} main
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv0 ON split(main.path,'/')[offset(0)] = lv0.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv1 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 1, split(main.path,'/')[offset(1)], null) = lv1.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv2 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 2, split(main.path,'/')[offset(2)], null) = lv2.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv3 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 3, split(main.path,'/')[offset(3)], null) = lv3.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv4 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 4, split(main.path,'/')[offset(4)], null) = lv4.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv5 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 5, split(main.path,'/')[offset(5)], null) = lv5.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv6 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 6, split(main.path,'/')[offset(6)], null) = lv6.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv7 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 7, split(main.path,'/')[offset(7)], null) = lv7.entity_id
