SELECT
    item_id,
    product_id,
    stock_id,
    qty,
    min_qty,
    use_config_min_qty,
    is_qty_decimal,
    backorders,
    use_config_backorders,
    min_sale_qty,
    use_config_min_sale_qty,
    max_sale_qty,
    use_config_max_sale_qty,
    is_in_stock,CASE
        WHEN low_stock_date = '0000-00-00 00:00:00' THEN NULL
        ELSE low_stock_date
    END low_stock_date,
    notify_stock_qty,
    use_config_notify_stock_qty,
    manage_stock,
    use_config_manage_stock,
    stock_status_changed_auto,
    use_config_qty_increments,
    qty_increments,
    use_config_enable_qty_inc,
    enable_qty_increments,
    is_decimal_divided
FROM
    {{ ref(
        'cataloginventory_stock_item'
    ) }}
