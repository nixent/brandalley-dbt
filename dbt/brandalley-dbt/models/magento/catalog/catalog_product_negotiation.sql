SELECT
    negotiation_id,
    updated_at,
    PARENT,
    sap_ref,
    TYPE,
    supplier,
    currency,
    rate,
    cost,
    price,
    status,
    admin,
    date_import,
    CASE
        WHEN date_reservation = '0000-00-00 00:00:00' THEN NULL
        ELSE date_reservation
    END date_reservation,CASE
        WHEN date_pend_proc = '0000-00-00 00:00:00' THEN NULL
        ELSE date_pend_proc
    END date_pend_proc,CASE
        WHEN date_proc_comp = '0000-00-00 00:00:00' THEN NULL
        ELSE date_proc_comp
    END date_proc_comp,CASE
        WHEN date_comp_exported = '0000-00-00 00:00:00' THEN NULL
        ELSE date_comp_exported
    END date_comp_exported,CASE
        WHEN date_due = '0000-00-00 00:00:00' THEN NULL
        ELSE date_due
    END date_due,
    sap_message,
    buyer,
    department
FROM {{ ref('stg__catalog_product_negotiation') }}
