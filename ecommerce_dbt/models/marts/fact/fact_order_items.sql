SELECT
    oi.ORDER_ID,
    oi.ORDER_ITEM_ID,

    o.CUSTOMER_ID,
    oi.PRODUCT_ID,
    oi.SELLER_ID,

    o.ORDER_STATUS,
    o.ORDER_PURCHASE_TIMESTAMP,
    o.ORDER_PURCHASE_DATE,

    oi.SHIPPING_LIMIT_DATE,

    oi.PRICE,
    oi.FREIGHT_VALUE,

    oi.PRICE + oi.FREIGHT_VALUE AS ITEM_TOTAL_WITH_FREIGHT

FROM {{ ref('stg_order_items') }} AS oi

INNER JOIN {{ ref('stg_orders') }} AS o
    ON oi.ORDER_ID = o.ORDER_ID