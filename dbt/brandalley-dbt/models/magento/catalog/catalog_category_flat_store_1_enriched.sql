select main.*,
concat(lv0.name,IF (lv1.name IS NOT NULL,concat('>',lv1.name),''),IF (lv2.name IS NOT NULL,concat('>',lv2.name),''), IF (lv3.name IS NOT NULL,concat('>',lv3.name),''), IF (lv4.name IS NOT NULL,concat('>',lv4.name),''), IF (lv5.name IS NOT NULL,concat('>',lv5.name),''), IF (lv6.name IS NOT NULL,concat('>',lv6.name),''), IF (lv7.name IS NOT NULL,concat('>',lv7.name),'')) path_name
FROM {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} main
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv0 ON SUBSTRING_INDEX (main.path,'/',1) = lv0.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv1 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 0, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',2), '/', -1), null) = lv1.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv2 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 1, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',3), '/', -1), null) = lv2.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv3 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 2, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',4), '/', -1), null) = lv3.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv4 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 3, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',5), '/', -1), null) = lv4.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv5 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 4, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',6), '/', -1), null) = lv5.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv6 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 5, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',7), '/', -1), null) = lv6.entity_id
  LEFT OUTER JOIN {{ ref(
				'stg__catalog_category_flat_store_1'
		) }} lv7 ON IF(LENGTH(main.path)
            - LENGTH( REPLACE ( main.path, "/", "") )> 6, SUBSTRING_INDEX(SUBSTRING_INDEX (main.path,'/',8), '/', -1), null) = lv7.entity_id
