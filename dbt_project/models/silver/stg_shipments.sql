-- Silver: Cleaned and validated shipments with delivery performance metrics
-- Source: COCO_DE_DEMO.BRONZE.SHIPMENTS

SELECT
    SHIPMENT_ID,
    ORDER_ID,
    UPPER(TRIM(CARRIER))                                      AS CARRIER,
    TRIM(TRACKING_NUMBER)                                     AS TRACKING_NUMBER,
    SHIP_DATE,
    DELIVERY_DATE,
    UPPER(TRIM(STATUS))                                       AS STATUS,
    CASE
        WHEN DELIVERY_DATE IS NOT NULL
        THEN DATEDIFF('day', SHIP_DATE, DELIVERY_DATE)
        ELSE NULL
    END                                                       AS DELIVERY_DAYS,
    CASE
        WHEN DELIVERY_DATE IS NOT NULL
         AND DATEDIFF('day', SHIP_DATE, DELIVERY_DATE) > 7
        THEN TRUE
        ELSE FALSE
    END                                                       AS IS_DELAYED,
    CASE
        WHEN UPPER(TRIM(STATUS)) = 'DELIVERED' THEN TRUE
        ELSE FALSE
    END                                                       AS IS_DELIVERED,
    CASE
        WHEN DELIVERY_DATE IS NULL THEN 'PENDING'
        WHEN DATEDIFF('day', SHIP_DATE, DELIVERY_DATE) <= 1 THEN 'SAME_DAY'
        WHEN DATEDIFF('day', SHIP_DATE, DELIVERY_DATE) <= 3 THEN 'FAST'
        WHEN DATEDIFF('day', SHIP_DATE, DELIVERY_DATE) <= 7 THEN 'STANDARD'
        ELSE 'SLOW'
    END                                                       AS SHIPPING_SPEED,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM COCO_DE_DEMO.BRONZE.SHIPMENTS
WHERE SHIPMENT_ID IS NOT NULL
  AND ORDER_ID IS NOT NULL
  AND SHIP_DATE IS NOT NULL
