-- Silver: Cleaned and validated payments with method grouping and timing
-- Source: COCO_DE_DEMO.BRONZE.PAYMENTS, COCO_DE_DEMO.BRONZE.ORDERS

WITH orders AS (
    SELECT ORDER_ID, ORDER_DATE
    FROM COCO_DE_DEMO.BRONZE.ORDERS
    WHERE ORDER_ID IS NOT NULL
)

SELECT
    py.PAYMENT_ID,
    py.ORDER_ID,
    LOWER(TRIM(py.PAYMENT_METHOD))                            AS PAYMENT_METHOD,
    CASE
        WHEN LOWER(TRIM(py.PAYMENT_METHOD)) IN ('credit_card', 'debit_card') THEN 'CARD'
        WHEN LOWER(TRIM(py.PAYMENT_METHOD)) IN ('paypal', 'google_pay')      THEN 'DIGITAL'
        WHEN LOWER(TRIM(py.PAYMENT_METHOD)) = 'gift_card'                    THEN 'GIFT_CARD'
        ELSE 'OTHER'
    END                                                       AS PAYMENT_METHOD_GROUP,
    py.AMOUNT,
    py.PAYMENT_DATE,
    UPPER(TRIM(py.STATUS))                                    AS STATUS,
    CASE
        WHEN UPPER(TRIM(py.STATUS)) = 'COMPLETED' THEN TRUE
        ELSE FALSE
    END                                                       AS IS_SUCCESSFUL,
    CASE
        WHEN o.ORDER_DATE IS NOT NULL
        THEN DATEDIFF('day', o.ORDER_DATE, py.PAYMENT_DATE)
        ELSE NULL
    END                                                       AS DAYS_TO_PAYMENT,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM COCO_DE_DEMO.BRONZE.PAYMENTS py
LEFT JOIN orders o ON py.ORDER_ID = o.ORDER_ID
WHERE py.PAYMENT_ID IS NOT NULL
  AND py.ORDER_ID IS NOT NULL
  AND py.AMOUNT >= 0
