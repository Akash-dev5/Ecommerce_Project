SELECT
    -- Order-item identifiers
    oi.ORDER_ID,
    oi.ORDER_ITEM_ID,

    -- Customer attributes
    fo.CUSTOMER_ID,
    dc.CUSTOMER_UNIQUE_ID,
    dc.CUSTOMER_ZIP_CODE_PREFIX,
    dc.CUSTOMER_CITY,
    dc.CUSTOMER_STATE,

    -- Order attributes
    fo.ORDER_STATUS,
    fo.STATUS_CATEGORY,
    fo.ORDER_PURCHASE_TIMESTAMP,
    fo.ORDER_PURCHASE_DATE,
    fo.ORDER_APPROVED_AT,
    fo.ORDER_DELIVERED_CARRIER_DATE,
    fo.ORDER_DELIVERED_CUSTOMER_DATE,
    fo.ORDER_ESTIMATED_DELIVERY_DATE,

    -- Product attributes
    oi.PRODUCT_ID,
    dp.PRODUCT_CATEGORY_NAME,
    dp.PRODUCT_CATEGORY_NAME_ENGLISH,
    dp.PRODUCT_NAME_LENGTH,
    dp.PRODUCT_DESCRIPTION_LENGTH,
    dp.PRODUCT_PHOTOS_QTY,
    dp.PRODUCT_WEIGHT_G,
    dp.PRODUCT_LENGTH_CM,
    dp.PRODUCT_HEIGHT_CM,
    dp.PRODUCT_WIDTH_CM,

    -- Seller attributes
    oi.SELLER_ID,
    ds.SELLER_ZIP_CODE_PREFIX,
    ds.SELLER_CITY,
    ds.SELLER_STATE,

    -- Order-item measures
    oi.SHIPPING_LIMIT_DATE,
    oi.PRICE AS ITEM_PRICE,
    oi.FREIGHT_VALUE AS ITEM_FREIGHT_VALUE,
    oi.ITEM_TOTAL_WITH_FREIGHT,

    -- Order-level measures
    fo.ORDER_ITEM_COUNT,
    fo.TOTAL_ITEM_PRICE AS ORDER_TOTAL_ITEM_PRICE,
    fo.TOTAL_FREIGHT AS ORDER_TOTAL_FREIGHT,
    fo.TOTAL_ITEM_VALUE_WITH_FREIGHT AS ORDER_TOTAL_WITH_FREIGHT,
    fo.PAYMENT_COUNT,
    fo.TOTAL_PAYMENT_VALUE AS ORDER_TOTAL_PAYMENT_VALUE,
    fo.REVIEW_COUNT,
    fo.AVG_REVIEW_SCORE

FROM {{ ref('fact_order_items') }} AS oi

INNER JOIN {{ ref('fact_orders') }} AS fo
    ON oi.ORDER_ID = fo.ORDER_ID

LEFT JOIN {{ ref('dim_customers') }} AS dc
    ON fo.CUSTOMER_ID = dc.CUSTOMER_ID

LEFT JOIN {{ ref('dim_products') }} AS dp
    ON oi.PRODUCT_ID = dp.PRODUCT_ID

LEFT JOIN {{ ref('dim_sellers') }} AS ds
    ON oi.SELLER_ID = ds.SELLER_ID