-- Gold: Product dimension with sales performance, margin, and inventory metrics
-- Sources: Silver stg_products, stg_order_items, stg_orders

WITH product_sales AS (
    SELECT
        oi.PRODUCT_ID,
        COUNT(DISTINCT oi.ORDER_ID)                            AS ORDERS_WITH_PRODUCT,
        SUM(oi.QUANTITY)                                       AS TOTAL_UNITS_SOLD,
        SUM(oi.LINE_TOTAL)                                     AS TOTAL_REVENUE,
        SUM(oi.ITEM_MARGIN)                                    AS TOTAL_MARGIN,
        AVG(oi.UNIT_PRICE)                                     AS AVG_SELLING_PRICE,
        AVG(oi.DISCOUNT)                                       AS AVG_DISCOUNT,
        SUM(CASE WHEN oi.IS_DISCOUNTED THEN oi.QUANTITY ELSE 0 END) AS DISCOUNTED_UNITS,
        AVG(oi.DISCOUNT_PCT)                                   AS AVG_DISCOUNT_PCT,
        CASE
            WHEN COUNT(DISTINCT oi.ORDER_ID) > 0
            THEN ROUND(SUM(oi.QUANTITY)::FLOAT / COUNT(DISTINCT oi.ORDER_ID), 2)
            ELSE 0
        END                                                    AS AVG_UNITS_PER_ORDER,
        MIN(o.ORDER_DATE)                                      AS FIRST_SOLD_DATE,
        MAX(o.ORDER_DATE)                                      AS LAST_SOLD_DATE,
        DATEDIFF('day', MIN(o.ORDER_DATE), MAX(o.ORDER_DATE))  AS SELLING_SPAN_DAYS
    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.ORDER_ID = o.ORDER_ID
    GROUP BY oi.PRODUCT_ID
),

return_exposure AS (
    SELECT
        oi.PRODUCT_ID,
        SUM(oi.LINE_TOTAL)                                     AS RETURNED_REVENUE
    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.ORDER_ID = o.ORDER_ID
    WHERE o.STATUS = 'RETURNED'
    GROUP BY oi.PRODUCT_ID
)

SELECT
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUBCATEGORY,
    p.BRAND,
    p.UNIT_PRICE,
    p.COST_PRICE,
    p.PROFIT_MARGIN,
    p.MARGIN_PCT,
    p.PRICE_TIER,
    p.STOCK_QUANTITY,
    p.IS_LOW_STOCK,
    COALESCE(ps.TOTAL_UNITS_SOLD, 0)                          AS TOTAL_UNITS_SOLD,
    COALESCE(ps.TOTAL_REVENUE, 0)                              AS TOTAL_REVENUE,
    COALESCE(ps.TOTAL_MARGIN, 0)                               AS TOTAL_MARGIN,
    COALESCE(ps.ORDERS_WITH_PRODUCT, 0)                        AS ORDERS_WITH_PRODUCT,
    COALESCE(ps.AVG_SELLING_PRICE, 0)                          AS AVG_SELLING_PRICE,
    COALESCE(ps.AVG_DISCOUNT, 0)                               AS AVG_DISCOUNT,
    COALESCE(ps.AVG_DISCOUNT_PCT, 0)                           AS AVG_DISCOUNT_PCT,
    COALESCE(ps.DISCOUNTED_UNITS, 0)                           AS DISCOUNTED_UNITS,
    COALESCE(ps.AVG_UNITS_PER_ORDER, 0)                        AS AVG_UNITS_PER_ORDER,
    ps.FIRST_SOLD_DATE,
    ps.LAST_SOLD_DATE,
    COALESCE(ps.SELLING_SPAN_DAYS, 0)                          AS SELLING_SPAN_DAYS,
    COALESCE(re.RETURNED_REVENUE, 0)                           AS RETURNED_REVENUE,
    CASE
        WHEN ps.TOTAL_UNITS_SOLD > 0 AND p.STOCK_QUANTITY > 0
        THEN ROUND(ps.TOTAL_UNITS_SOLD::FLOAT / p.STOCK_QUANTITY, 2)
        ELSE 0
    END                                                        AS INVENTORY_TURNOVER,
    CASE
        WHEN ps.TOTAL_UNITS_SOLD > 0 AND ps.SELLING_SPAN_DAYS > 0
        THEN ROUND(
            p.STOCK_QUANTITY::FLOAT / (ps.TOTAL_UNITS_SOLD::FLOAT / ps.SELLING_SPAN_DAYS), 0
        )
        ELSE NULL
    END                                                        AS DAYS_OF_STOCK_REMAINING,
    CASE
        WHEN ps.TOTAL_REVENUE > 50000 THEN 'TOP_SELLER'
        WHEN ps.TOTAL_REVENUE > 10000 THEN 'STRONG'
        WHEN ps.TOTAL_REVENUE > 0     THEN 'ACTIVE'
        ELSE 'NO_SALES'
    END                                                        AS SALES_TIER,
    p.CREATED_AT,
    CURRENT_TIMESTAMP()                                        AS _LOADED_AT
FROM {{ ref('stg_products') }} p
LEFT JOIN product_sales ps ON p.PRODUCT_ID = ps.PRODUCT_ID
LEFT JOIN return_exposure re ON p.PRODUCT_ID = re.PRODUCT_ID
