-- Gold: Payment summary fact table
-- Grain: one row per order
-- Sources: stg_payments, stg_orders

WITH payment_agg AS (
    SELECT
        ORDER_ID,
        COUNT(*)                                               AS TOTAL_ATTEMPTS,
        SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS COMPLETED_COUNT,
        SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END)    AS FAILED_COUNT,
        SUM(CASE WHEN STATUS = 'REFUNDED' THEN 1 ELSE 0 END)  AS REFUNDED_COUNT,
        SUM(CASE WHEN STATUS = 'PENDING' THEN 1 ELSE 0 END)   AS PENDING_COUNT,
        SUM(AMOUNT)                                            AS TOTAL_AMOUNT_ATTEMPTED,
        SUM(CASE WHEN STATUS = 'COMPLETED' THEN AMOUNT ELSE 0 END) AS TOTAL_AMOUNT_COMPLETED,
        SUM(CASE WHEN STATUS = 'FAILED' THEN AMOUNT ELSE 0 END)    AS TOTAL_AMOUNT_FAILED,
        SUM(CASE WHEN STATUS = 'REFUNDED' THEN AMOUNT ELSE 0 END)  AS TOTAL_AMOUNT_REFUNDED,
        ROUND(
            SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                                      AS PAYMENT_SUCCESS_RATE,
        LISTAGG(DISTINCT PAYMENT_METHOD, ', ') WITHIN GROUP (ORDER BY PAYMENT_METHOD)
                                                               AS PAYMENT_METHODS_USED,
        LISTAGG(DISTINCT PAYMENT_METHOD_GROUP, ', ') WITHIN GROUP (ORDER BY PAYMENT_METHOD_GROUP)
                                                               AS PAYMENT_GROUPS_USED,
        MIN(PAYMENT_DATE)                                      AS FIRST_PAYMENT_DATE,
        MAX(PAYMENT_DATE)                                      AS LAST_PAYMENT_DATE
    FROM {{ ref('stg_payments') }}
    GROUP BY ORDER_ID
)

SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_DATE,
    o.STATUS                                                  AS ORDER_STATUS,
    o.CHANNEL,
    o.TOTAL_AMOUNT                                            AS ORDER_TOTAL,
    COALESCE(pa.TOTAL_ATTEMPTS, 0)                            AS TOTAL_PAYMENT_ATTEMPTS,
    COALESCE(pa.COMPLETED_COUNT, 0)                           AS COMPLETED_PAYMENTS,
    COALESCE(pa.FAILED_COUNT, 0)                              AS FAILED_PAYMENTS,
    COALESCE(pa.REFUNDED_COUNT, 0)                            AS REFUNDED_PAYMENTS,
    COALESCE(pa.PENDING_COUNT, 0)                             AS PENDING_PAYMENTS,
    COALESCE(pa.TOTAL_AMOUNT_ATTEMPTED, 0)                    AS TOTAL_AMOUNT_ATTEMPTED,
    COALESCE(pa.TOTAL_AMOUNT_COMPLETED, 0)                    AS TOTAL_AMOUNT_COMPLETED,
    COALESCE(pa.TOTAL_AMOUNT_FAILED, 0)                       AS TOTAL_AMOUNT_FAILED,
    COALESCE(pa.TOTAL_AMOUNT_REFUNDED, 0)                     AS TOTAL_AMOUNT_REFUNDED,
    COALESCE(pa.PAYMENT_SUCCESS_RATE, 0)                      AS PAYMENT_SUCCESS_RATE,
    pa.PAYMENT_METHODS_USED,
    pa.PAYMENT_GROUPS_USED,
    pa.FIRST_PAYMENT_DATE,
    pa.LAST_PAYMENT_DATE,
    CASE
        WHEN pa.FAILED_COUNT > 0 AND pa.COMPLETED_COUNT > 0 THEN 'RECOVERED'
        WHEN pa.FAILED_COUNT > 0 AND pa.COMPLETED_COUNT = 0 THEN 'ALL_FAILED'
        WHEN pa.REFUNDED_COUNT > 0                           THEN 'REFUNDED'
        WHEN pa.COMPLETED_COUNT > 0                          THEN 'CLEAN'
        ELSE 'NO_PAYMENT'
    END                                                       AS PAYMENT_OUTCOME,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM {{ ref('stg_orders') }} o
LEFT JOIN payment_agg pa ON o.ORDER_ID = pa.ORDER_ID
