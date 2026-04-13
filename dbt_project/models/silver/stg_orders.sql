-- Silver: Cleaned and validated orders with derived attributes
-- Source: COCO_DE_DEMO.BRONZE.ORDERS

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    UPPER(TRIM(STATUS))                                       AS STATUS,
    LOWER(TRIM(CHANNEL))                                      AS CHANNEL,
    TOTAL_AMOUNT,
    DATEDIFF('day', ORDER_DATE, CURRENT_DATE())               AS DAYS_SINCE_ORDER,
    CASE
        WHEN TOTAL_AMOUNT > 5000 THEN TRUE
        ELSE FALSE
    END                                                       AS IS_HIGH_VALUE,
    DAYNAME(ORDER_DATE)                                       AS ORDER_DAY_OF_WEEK,
    CASE
        WHEN DAYOFWEEK(ORDER_DATE) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                                       AS IS_WEEKEND_ORDER,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM COCO_DE_DEMO.BRONZE.ORDERS
WHERE ORDER_ID IS NOT NULL
  AND CUSTOMER_ID IS NOT NULL
  AND TOTAL_AMOUNT >= 0
