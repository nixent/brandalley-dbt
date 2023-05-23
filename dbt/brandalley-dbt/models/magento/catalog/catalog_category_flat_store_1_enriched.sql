select 
	main.*,
	concat(lv0.name,
	IF (lv1.name IS NOT NULL,concat('>',lv1.name),''),
	IF (lv2.name IS NOT NULL,concat('>',lv2.name),''), 
	IF (lv3.name IS NOT NULL,concat('>',lv3.name),''), 
	IF (lv4.name IS NOT NULL,concat('>',lv4.name),''), 
	IF (lv5.name IS NOT NULL,concat('>',lv5.name),''), 
	IF (lv6.name IS NOT NULL,concat('>',lv6.name),''), 
	IF (lv7.name IS NOT NULL,concat('>',lv7.name),'')
	) as path_name
FROM {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} main
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv0 ON cast(split(main.path,'/')[offset(0)] as int) = lv0.entity_id and main.ba_site = lv0.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv1 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 1, cast(split(main.path,'/')[offset(1)] as int), null) = lv1.entity_id and main.ba_site = lv1.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv2 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 2, cast(split(main.path,'/')[offset(2)] as int), null) = lv2.entity_id and main.ba_site = lv2.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv3 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 3, cast(split(main.path,'/')[offset(3)] as int), null) = lv3.entity_id and main.ba_site = lv3.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv4 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 4, cast(split(main.path,'/')[offset(4)] as int), null) = lv4.entity_id and main.ba_site = lv4.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv5 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 5, cast(split(main.path,'/')[offset(5)] as int), null) = lv5.entity_id and main.ba_site = lv5.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv6 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 6, cast(split(main.path,'/')[offset(6)] as int), null) = lv6.entity_id and main.ba_site = lv6.ba_site
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv7 ON IF(ARRAY_LENGTH(split(main.path,'/'))> 7, cast(split(main.path,'/')[offset(7)] as int), null) = lv7.entity_id and main.ba_site = lv7.ba_site
