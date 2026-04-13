-- Gold: Date dimension table spanning the full order date range
-- Generated from stg_orders date boundaries

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), (SELECT MIN(ORDER_DATE)::DATE FROM {{ ref('stg_orders') }})) AS DATE_KEY
    FROM TABLE(GENERATOR(ROWCOUNT => 1500))
),
filtered_spine AS (
    SELECT DATE_KEY
    FROM date_spine
    WHERE DATE_KEY <= (SELECT MAX(ORDER_DATE)::DATE FROM {{ ref('stg_orders') }})
)

SELECT
    DATE_KEY,
    DATE_KEY                                                  AS FULL_DATE,
    DAYOFWEEK(DATE_KEY)                                       AS DAY_OF_WEEK,
    DAYNAME(DATE_KEY)                                         AS DAY_NAME,
    CASE
        WHEN DAYOFWEEK(DATE_KEY) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                                       AS IS_WEEKEND,
    DAY(DATE_KEY)                                             AS DAY_OF_MONTH,
    WEEKOFYEAR(DATE_KEY)                                      AS WEEK_OF_YEAR,
    MONTH(DATE_KEY)                                           AS MONTH_NUM,
    MONTHNAME(DATE_KEY)                                       AS MONTH_NAME,
    QUARTER(DATE_KEY)                                         AS QUARTER_NUM,
    'Q' || QUARTER(DATE_KEY)                                  AS QUARTER_NAME,
    YEAR(DATE_KEY)                                            AS YEAR_NUM,
    'FQ' || CASE
        WHEN MONTH(DATE_KEY) IN (1, 2, 3) THEN '4'
        WHEN MONTH(DATE_KEY) IN (4, 5, 6) THEN '1'
        WHEN MONTH(DATE_KEY) IN (7, 8, 9) THEN '2'
        ELSE '3'
    END                                                       AS FISCAL_QUARTER,
    DATE_TRUNC('MONTH', DATE_KEY)                             AS FIRST_DAY_OF_MONTH,
    LAST_DAY(DATE_KEY, 'MONTH')                               AS LAST_DAY_OF_MONTH,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM filtered_spine
