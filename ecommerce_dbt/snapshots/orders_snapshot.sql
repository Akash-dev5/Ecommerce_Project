{% snapshot orders_snapshot %}

{{
    config(
        unique_key='ORDER_ID',
        strategy='check',
        check_cols=[
            'ORDER_STATUS',
            'ORDER_APPROVED_AT',
            'ORDER_DELIVERED_CARRIER_DATE',
            'ORDER_DELIVERED_CUSTOMER_DATE'
        ]
    )
}}

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_PURCHASE_TIMESTAMP,
    ORDER_APPROVED_AT,
    ORDER_DELIVERED_CARRIER_DATE,
    ORDER_DELIVERED_CUSTOMER_DATE,
    ORDER_ESTIMATED_DELIVERY_DATE,
    ORDER_PURCHASE_DATE

FROM {{ ref('stg_orders') }}

{% endsnapshot %}