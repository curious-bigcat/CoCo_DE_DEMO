-- Gold: Shipment performance fact table
-- Grain: one row per shipment
-- Sources: stg_shipments, stg_orders, stg_customers

SELECT
    s.SHIPMENT_ID,
    s.ORDER_ID,
    o.CUSTOMER_ID,
    c.SEGMENT                                                 AS CUSTOMER_SEGMENT,
    c.STATE                                                   AS CUSTOMER_STATE,
    o.CHANNEL                                                 AS ORDER_CHANNEL,
    o.TOTAL_AMOUNT                                            AS ORDER_VALUE,
    o.IS_HIGH_VALUE                                           AS IS_HIGH_VALUE_ORDER,
    o.ORDER_DATE,
    s.SHIP_DATE,
    s.DELIVERY_DATE,
    s.CARRIER,
    s.STATUS                                                  AS SHIPMENT_STATUS,
    s.TRACKING_NUMBER,
    s.DELIVERY_DAYS,
    s.IS_DELAYED,
    s.IS_DELIVERED,
    s.SHIPPING_SPEED,
    DATEDIFF('day', o.ORDER_DATE, s.SHIP_DATE)                AS DAYS_ORDER_TO_SHIP,
    CASE
        WHEN s.DELIVERY_DATE IS NOT NULL
        THEN DATEDIFF('day', o.ORDER_DATE, s.DELIVERY_DATE)
        ELSE NULL
    END                                                       AS DAYS_ORDER_TO_DELIVERY,
    DATE_TRUNC('MONTH', s.SHIP_DATE)                          AS SHIP_MONTH,
    YEAR(s.SHIP_DATE)                                         AS SHIP_YEAR,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM {{ ref('stg_shipments') }} s
INNER JOIN {{ ref('stg_orders') }} o ON s.ORDER_ID = o.ORDER_ID
LEFT JOIN {{ ref('stg_customers') }} c ON o.CUSTOMER_ID = c.CUSTOMER_ID
