-- Silver: Cleaned and standardized customers with derived attributes
-- Source: COCO_DE_DEMO.BRONZE.CUSTOMERS

SELECT
    CUSTOMER_ID,
    TRIM(FIRST_NAME)                                          AS FIRST_NAME,
    TRIM(LAST_NAME)                                           AS LAST_NAME,
    TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)               AS FULL_NAME,
    LOWER(TRIM(EMAIL))                                        AS EMAIL,
    SPLIT_PART(LOWER(TRIM(EMAIL)), '@', 2)                    AS EMAIL_DOMAIN,
    TRIM(PHONE)                                               AS PHONE,
    TRIM(CITY)                                                AS CITY,
    UPPER(TRIM(STATE))                                        AS STATE,
    TRIM(ZIP_CODE)                                            AS ZIP_CODE,
    UPPER(TRIM(SEGMENT))                                      AS SEGMENT,
    CREATED_AT,
    DATEDIFF('day', CREATED_AT, CURRENT_DATE())               AS CUSTOMER_TENURE_DAYS,
    CASE
        WHEN DATEDIFF('day', CREATED_AT, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END                                                       AS IS_ACTIVE,
    CURRENT_TIMESTAMP()                                       AS _LOADED_AT
FROM COCO_DE_DEMO.BRONZE.CUSTOMERS
WHERE CUSTOMER_ID IS NOT NULL
  AND EMAIL IS NOT NULL
