-- Silver: Cleaned and validated order items with margin and discount analysis
-- Source: COCO_DE_DEMO.BRONZE.ORDER_ITEMS, COCO_DE_DEMO.BRONZE.PRODUCTS

WITH products AS (
    SELECT PRODUCT_ID, COST_PRICE
    FROM COCO_DE_DEMO.BRONZE.PRODUCTS
    WHERE PRODUCT_ID IS NOT NULL
)

SELECT
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID,
    oi.PRODUCT_ID,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    COALESCE(oi.DISCOUNT, 0)                                  AS DISCOUNT,
    (oi.QUANTITY * oi.UNIT_PRICE) - COALESCE(oi.DISCOUNT, 0)  AS LINE_TOTAL,
    CASE
        WHEN oi.QUANTITY * oi.UNIT_PRICE > 0
        THEN ROUND(COALESCE(oi.DISCOUNT, 0) / (oi.QUANTITY * oi.UNIT_PRICE) * 100, 2)
        ELSE 0
    END                                                        AS DISCOUNT_PCT,
    CASE
        WHEN COALESCE(oi.DISCOUNT, 0) > 0 THEN TRUE
        ELSE FALSE
    END                                                        AS IS_DISCOUNTED,
    (oi.QUANTITY * oi.UNIT_PRICE) - COALESCE(oi.DISCOUNT, 0)
      - (oi.QUANTITY * COALESCE(p.COST_PRICE, 0))             AS ITEM_MARGIN,
    CURRENT_TIMESTAMP()                                        AS _LOADED_AT
FROM COCO_DE_DEMO.BRONZE.ORDER_ITEMS oi
LEFT JOIN products p ON oi.PRODUCT_ID = p.PRODUCT_ID
WHERE oi.ORDER_ITEM_ID IS NOT NULL
  AND oi.ORDER_ID IS NOT NULL
  AND oi.QUANTITY > 0
  AND oi.UNIT_PRICE >= 0
