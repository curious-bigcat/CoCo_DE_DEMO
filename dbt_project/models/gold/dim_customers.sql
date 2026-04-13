-- Gold: Customer dimension with order summary, behavioral, and value metrics
-- Sources: Silver stg_customers, stg_orders, stg_payments

WITH customer_orders AS (
    SELECT
        CUSTOMER_ID,
        COUNT(DISTINCT ORDER_ID)                               AS TOTAL_ORDERS,
        SUM(TOTAL_AMOUNT)                                      AS LIFETIME_SPEND,
        AVG(TOTAL_AMOUNT)                                      AS AVG_ORDER_VALUE,
        MIN(ORDER_DATE)                                        AS FIRST_ORDER_DATE,
        MAX(ORDER_DATE)                                        AS LAST_ORDER_DATE,
        DATEDIFF('day', MAX(ORDER_DATE), CURRENT_DATE())       AS DAYS_SINCE_LAST_ORDER,
        COUNT(DISTINCT CASE WHEN STATUS = 'COMPLETED' THEN ORDER_ID END)  AS COMPLETED_ORDERS,
        COUNT(DISTINCT CASE WHEN STATUS = 'CANCELLED' THEN ORDER_ID END)  AS CANCELLED_ORDERS,
        COUNT(DISTINCT CASE WHEN STATUS = 'RETURNED' THEN ORDER_ID END)   AS RETURNED_ORDERS,
        SUM(CASE WHEN IS_HIGH_VALUE THEN 1 ELSE 0 END)        AS HIGH_VALUE_ORDER_COUNT,
        SUM(CASE WHEN IS_WEEKEND_ORDER THEN 1 ELSE 0 END)     AS WEEKEND_ORDER_COUNT
    FROM {{ ref('stg_orders') }}
    GROUP BY CUSTOMER_ID
),

customer_payments AS (
    SELECT
        o.CUSTOMER_ID,
        COUNT(DISTINCT p.PAYMENT_ID)                           AS TOTAL_PAYMENTS,
        SUM(CASE WHEN p.STATUS = 'COMPLETED' THEN p.AMOUNT ELSE 0 END) AS TOTAL_PAID,
        SUM(CASE WHEN p.STATUS = 'REFUNDED' THEN p.AMOUNT ELSE 0 END)  AS TOTAL_REFUNDED
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_orders') }} o ON p.ORDER_ID = o.ORDER_ID
    GROUP BY o.CUSTOMER_ID
),

preferred_channel AS (
    SELECT
        CUSTOMER_ID,
        CHANNEL                                                AS PREFERRED_CHANNEL
    FROM (
        SELECT
            CUSTOMER_ID,
            CHANNEL,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) AS rn
        FROM {{ ref('stg_orders') }}
        GROUP BY CUSTOMER_ID, CHANNEL
    )
    WHERE rn = 1
)

SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.FULL_NAME,
    c.EMAIL,
    c.EMAIL_DOMAIN,
    c.CITY,
    c.STATE,
    c.ZIP_CODE,
    c.SEGMENT,
    c.CREATED_AT                                               AS CUSTOMER_SINCE,
    c.CUSTOMER_TENURE_DAYS,
    c.IS_ACTIVE,
    COALESCE(co.TOTAL_ORDERS, 0)                               AS TOTAL_ORDERS,
    COALESCE(co.LIFETIME_SPEND, 0)                             AS LIFETIME_SPEND,
    COALESCE(co.AVG_ORDER_VALUE, 0)                            AS AVG_ORDER_VALUE,
    co.FIRST_ORDER_DATE,
    co.LAST_ORDER_DATE,
    COALESCE(co.DAYS_SINCE_LAST_ORDER, NULL)                   AS DAYS_SINCE_LAST_ORDER,
    COALESCE(co.COMPLETED_ORDERS, 0)                           AS COMPLETED_ORDERS,
    COALESCE(co.CANCELLED_ORDERS, 0)                           AS CANCELLED_ORDERS,
    COALESCE(co.RETURNED_ORDERS, 0)                            AS RETURNED_ORDERS,
    COALESCE(co.HIGH_VALUE_ORDER_COUNT, 0)                     AS HIGH_VALUE_ORDER_COUNT,
    COALESCE(co.WEEKEND_ORDER_COUNT, 0)                        AS WEEKEND_ORDER_COUNT,
    CASE
        WHEN co.TOTAL_ORDERS > 0
        THEN ROUND(co.CANCELLED_ORDERS::FLOAT / co.TOTAL_ORDERS * 100, 2)
        ELSE 0
    END                                                        AS CANCELLATION_RATE,
    CASE
        WHEN co.TOTAL_ORDERS > 1
             AND DATEDIFF('day', co.FIRST_ORDER_DATE, co.LAST_ORDER_DATE) > 0
        THEN ROUND(
            DATEDIFF('day', co.FIRST_ORDER_DATE, co.LAST_ORDER_DATE)::FLOAT
            / (co.TOTAL_ORDERS - 1), 1
        )
        ELSE NULL
    END                                                        AS AVG_DAYS_BETWEEN_ORDERS,
    CASE
        WHEN c.CUSTOMER_TENURE_DAYS > 0
        THEN ROUND(COALESCE(co.TOTAL_ORDERS, 0)::FLOAT / (c.CUSTOMER_TENURE_DAYS / 30.0), 2)
        ELSE 0
    END                                                        AS ORDERS_PER_MONTH,
    pc.PREFERRED_CHANNEL,
    COALESCE(cp.TOTAL_PAID, 0)                                 AS TOTAL_PAID,
    COALESCE(cp.TOTAL_REFUNDED, 0)                             AS TOTAL_REFUNDED,
    COALESCE(cp.TOTAL_PAID, 0) - COALESCE(cp.TOTAL_REFUNDED, 0) AS NET_PAID,
    CASE
        WHEN co.TOTAL_ORDERS >= 10 AND co.LIFETIME_SPEND >= 50000 THEN 'PLATINUM'
        WHEN co.TOTAL_ORDERS >= 5  AND co.LIFETIME_SPEND >= 20000 THEN 'GOLD'
        WHEN co.TOTAL_ORDERS >= 2  AND co.LIFETIME_SPEND >= 5000  THEN 'SILVER'
        ELSE 'BRONZE'
    END                                                        AS LOYALTY_TIER,
    CURRENT_TIMESTAMP()                                        AS _LOADED_AT
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_orders co ON c.CUSTOMER_ID = co.CUSTOMER_ID
LEFT JOIN customer_payments cp ON c.CUSTOMER_ID = cp.CUSTOMER_ID
LEFT JOIN preferred_channel pc ON c.CUSTOMER_ID = pc.CUSTOMER_ID
