-- Gold: Pre-aggregated daily revenue metrics
-- Grain: one row per day
-- Sources: stg_orders, stg_order_items

WITH daily_orders AS (
    SELECT
        ORDER_DATE::DATE                                       AS REVENUE_DATE,
        COUNT(DISTINCT ORDER_ID)                               AS TOTAL_ORDERS,
        COUNT(DISTINCT CUSTOMER_ID)                            AS UNIQUE_CUSTOMERS,
        SUM(TOTAL_AMOUNT)                                      AS GROSS_REVENUE,
        AVG(TOTAL_AMOUNT)                                      AS AVG_ORDER_VALUE,
        MIN(TOTAL_AMOUNT)                                      AS MIN_ORDER_VALUE,
        MAX(TOTAL_AMOUNT)                                      AS MAX_ORDER_VALUE,
        SUM(CASE WHEN IS_HIGH_VALUE THEN 1 ELSE 0 END)        AS HIGH_VALUE_ORDERS,
        SUM(CASE WHEN IS_WEEKEND_ORDER THEN 1 ELSE 0 END)     AS WEEKEND_ORDERS,
        SUM(CASE WHEN STATUS = 'COMPLETED' THEN TOTAL_AMOUNT ELSE 0 END) AS COMPLETED_REVENUE,
        SUM(CASE WHEN STATUS = 'CANCELLED' THEN TOTAL_AMOUNT ELSE 0 END) AS CANCELLED_REVENUE,
        SUM(CASE WHEN STATUS = 'RETURNED' THEN TOTAL_AMOUNT ELSE 0 END)  AS RETURNED_REVENUE,
        COUNT(DISTINCT CASE WHEN CHANNEL = 'web' THEN ORDER_ID END)       AS WEB_ORDERS,
        COUNT(DISTINCT CASE WHEN CHANNEL = 'mobile' THEN ORDER_ID END)    AS MOBILE_ORDERS,
        COUNT(DISTINCT CASE WHEN CHANNEL = 'in-store' THEN ORDER_ID END)  AS INSTORE_ORDERS,
        COUNT(DISTINCT CASE WHEN CHANNEL = 'phone' THEN ORDER_ID END)     AS PHONE_ORDERS
    FROM {{ ref('stg_orders') }}
    GROUP BY ORDER_DATE::DATE
),

daily_items AS (
    SELECT
        o.ORDER_DATE::DATE                                     AS REVENUE_DATE,
        SUM(oi.QUANTITY)                                       AS TOTAL_UNITS_SOLD,
        SUM(oi.DISCOUNT)                                       AS TOTAL_DISCOUNT,
        SUM(oi.LINE_TOTAL)                                     AS TOTAL_LINE_REVENUE,
        SUM(oi.ITEM_MARGIN)                                    AS TOTAL_ITEM_MARGIN,
        COUNT(DISTINCT oi.PRODUCT_ID)                          AS UNIQUE_PRODUCTS_SOLD
    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.ORDER_ID = o.ORDER_ID
    GROUP BY o.ORDER_DATE::DATE
)

SELECT
    do.REVENUE_DATE,
    do.TOTAL_ORDERS,
    do.UNIQUE_CUSTOMERS,
    do.GROSS_REVENUE,
    do.AVG_ORDER_VALUE,
    do.MIN_ORDER_VALUE,
    do.MAX_ORDER_VALUE,
    do.HIGH_VALUE_ORDERS,
    do.WEEKEND_ORDERS,
    do.COMPLETED_REVENUE,
    do.CANCELLED_REVENUE,
    do.RETURNED_REVENUE,
    do.GROSS_REVENUE - do.CANCELLED_REVENUE - do.RETURNED_REVENUE AS NET_REVENUE,
    do.WEB_ORDERS,
    do.MOBILE_ORDERS,
    do.INSTORE_ORDERS,
    do.PHONE_ORDERS,
    COALESCE(di.TOTAL_UNITS_SOLD, 0)                          AS TOTAL_UNITS_SOLD,
    COALESCE(di.TOTAL_DISCOUNT, 0)                            AS TOTAL_DISCOUNT,
    COALESCE(di.TOTAL_LINE_REVENUE, 0)                        AS TOTAL_LINE_REVENUE,
    COALESCE(di.TOTAL_ITEM_MARGIN, 0)                         AS TOTAL_ITEM_MARGIN,
    COALESCE(di.UNIQUE_PRODUCTS_SOLD, 0)                      AS UNIQUE_PRODUCTS_SOLD,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM daily_orders do
LEFT JOIN daily_items di ON do.REVENUE_DATE = di.REVENUE_DATE
ORDER BY do.REVENUE_DATE
