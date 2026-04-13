-- Gold: Fact sales table - denormalized order-level grain with profitability and fulfillment metrics
-- Sources: Silver stg_orders, stg_order_items, stg_customers, stg_payments, stg_shipments

WITH order_items_agg AS (
    SELECT
        ORDER_ID,
        COUNT(*)                                               AS LINE_ITEM_COUNT,
        SUM(QUANTITY)                                          AS TOTAL_UNITS,
        SUM(LINE_TOTAL)                                        AS ITEMS_TOTAL,
        SUM(DISCOUNT)                                          AS TOTAL_DISCOUNT,
        SUM(ITEM_MARGIN)                                       AS TOTAL_ITEM_MARGIN,
        COUNT(DISTINCT PRODUCT_ID)                             AS UNIQUE_PRODUCTS,
        AVG(DISCOUNT_PCT)                                      AS AVG_DISCOUNT_PCT,
        SUM(CASE WHEN IS_DISCOUNTED THEN 1 ELSE 0 END)        AS DISCOUNTED_LINE_ITEMS
    FROM {{ ref('stg_order_items') }}
    GROUP BY ORDER_ID
),

payment_agg AS (
    SELECT
        ORDER_ID,
        COUNT(*)                                               AS PAYMENT_COUNT,
        SUM(CASE WHEN STATUS = 'COMPLETED' THEN AMOUNT ELSE 0 END)  AS AMOUNT_PAID,
        SUM(CASE WHEN STATUS = 'REFUNDED' THEN AMOUNT ELSE 0 END)   AS AMOUNT_REFUNDED,
        SUM(CASE WHEN STATUS = 'FAILED' THEN AMOUNT ELSE 0 END)     AS AMOUNT_FAILED,
        LISTAGG(DISTINCT PAYMENT_METHOD, ', ') WITHIN GROUP (ORDER BY PAYMENT_METHOD)       AS PAYMENT_METHODS,
        LISTAGG(DISTINCT PAYMENT_METHOD_GROUP, ', ') WITHIN GROUP (ORDER BY PAYMENT_METHOD_GROUP) AS PAYMENT_METHOD_GROUPS
    FROM {{ ref('stg_payments') }}
    GROUP BY ORDER_ID
),

shipment_info AS (
    SELECT
        ORDER_ID,
        MIN(SHIP_DATE)                                         AS FIRST_SHIP_DATE,
        MAX(DELIVERY_DATE)                                     AS LAST_DELIVERY_DATE,
        MAX(STATUS)                                            AS SHIPMENT_STATUS,
        AVG(DELIVERY_DAYS)                                     AS AVG_DELIVERY_DAYS,
        MAX(CASE WHEN IS_DELAYED THEN TRUE ELSE FALSE END)     AS HAS_DELAYED_SHIPMENT,
        MAX(SHIPPING_SPEED)                                    AS SLOWEST_SHIPPING_SPEED
    FROM {{ ref('stg_shipments') }}
    GROUP BY ORDER_ID
),

customer_order_counts AS (
    SELECT
        CUSTOMER_ID,
        COUNT(DISTINCT ORDER_ID)                               AS CUSTOMER_TOTAL_ORDERS
    FROM {{ ref('stg_orders') }}
    GROUP BY CUSTOMER_ID
)

SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    c.FULL_NAME                                                AS CUSTOMER_NAME,
    c.SEGMENT                                                  AS CUSTOMER_SEGMENT,
    c.STATE                                                    AS CUSTOMER_STATE,
    c.IS_ACTIVE                                                AS IS_ACTIVE_CUSTOMER,
    o.ORDER_DATE,
    o.STATUS                                                   AS ORDER_STATUS,
    o.CHANNEL,
    o.TOTAL_AMOUNT                                             AS ORDER_TOTAL,
    o.IS_HIGH_VALUE,
    o.IS_WEEKEND_ORDER,
    o.ORDER_DAY_OF_WEEK,

    COALESCE(oi.LINE_ITEM_COUNT, 0)                            AS LINE_ITEM_COUNT,
    COALESCE(oi.TOTAL_UNITS, 0)                                AS TOTAL_UNITS,
    COALESCE(oi.ITEMS_TOTAL, 0)                                AS ITEMS_TOTAL,
    COALESCE(oi.TOTAL_DISCOUNT, 0)                             AS TOTAL_DISCOUNT,
    COALESCE(oi.TOTAL_ITEM_MARGIN, 0)                          AS TOTAL_ITEM_MARGIN,
    COALESCE(oi.UNIQUE_PRODUCTS, 0)                            AS UNIQUE_PRODUCTS,
    COALESCE(oi.AVG_DISCOUNT_PCT, 0)                           AS AVG_DISCOUNT_PCT,
    COALESCE(oi.DISCOUNTED_LINE_ITEMS, 0)                      AS DISCOUNTED_LINE_ITEMS,

    COALESCE(pa.PAYMENT_COUNT, 0)                              AS PAYMENT_COUNT,
    COALESCE(pa.AMOUNT_PAID, 0)                                AS AMOUNT_PAID,
    COALESCE(pa.AMOUNT_REFUNDED, 0)                            AS AMOUNT_REFUNDED,
    COALESCE(pa.AMOUNT_FAILED, 0)                              AS AMOUNT_FAILED,
    pa.PAYMENT_METHODS,
    pa.PAYMENT_METHOD_GROUPS,

    COALESCE(pa.AMOUNT_PAID, 0) - COALESCE(pa.AMOUNT_REFUNDED, 0) AS NET_REVENUE,
    CASE
        WHEN COALESCE(pa.AMOUNT_PAID, 0) - COALESCE(pa.AMOUNT_REFUNDED, 0) > 0 THEN TRUE
        ELSE FALSE
    END                                                        AS IS_PROFITABLE,

    si.FIRST_SHIP_DATE,
    si.LAST_DELIVERY_DATE,
    si.SHIPMENT_STATUS,
    si.AVG_DELIVERY_DAYS,
    COALESCE(si.HAS_DELAYED_SHIPMENT, FALSE)                   AS HAS_DELAYED_SHIPMENT,
    si.SLOWEST_SHIPPING_SPEED,

    CASE
        WHEN si.FIRST_SHIP_DATE IS NOT NULL
        THEN DATEDIFF('day', o.ORDER_DATE, si.FIRST_SHIP_DATE)
        ELSE NULL
    END                                                        AS DAYS_TO_SHIP,
    CASE
        WHEN si.LAST_DELIVERY_DATE IS NOT NULL
        THEN DATEDIFF('day', o.ORDER_DATE, si.LAST_DELIVERY_DATE)
        ELSE NULL
    END                                                        AS DAYS_TO_DELIVER,

    CASE
        WHEN coc.CUSTOMER_TOTAL_ORDERS > 1 THEN TRUE
        ELSE FALSE
    END                                                        AS IS_REPEAT_CUSTOMER,

    DATE_TRUNC('MONTH', o.ORDER_DATE)                          AS ORDER_MONTH,
    DATE_TRUNC('QUARTER', o.ORDER_DATE)                        AS ORDER_QUARTER,
    YEAR(o.ORDER_DATE)                                         AS ORDER_YEAR,

    CURRENT_TIMESTAMP()                                        AS _LOADED_AT
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN order_items_agg oi ON o.ORDER_ID = oi.ORDER_ID
LEFT JOIN payment_agg pa ON o.ORDER_ID = pa.ORDER_ID
LEFT JOIN shipment_info si ON o.ORDER_ID = si.ORDER_ID
LEFT JOIN customer_order_counts coc ON o.CUSTOMER_ID = coc.CUSTOMER_ID
