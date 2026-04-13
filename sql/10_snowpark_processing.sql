-- ============================================================
-- 10: Snowpark Python Processing
-- Hybrid SQL + Python pipeline: profiling, anomaly detection, enrichment
-- ============================================================

-- ============================================================
-- Supporting Tables
-- ============================================================

-- Bronze: Data profiling log (populated by Snowpark Bronze Profiler)
CREATE TABLE IF NOT EXISTS COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG (
    PROFILE_RUN_ID      VARCHAR DEFAULT UUID_STRING(),
    TABLE_NAME          VARCHAR,
    COLUMN_NAME         VARCHAR,
    METRIC              VARCHAR,
    METRIC_VALUE        VARCHAR,
    PROFILED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver: Anomaly flags (populated by Snowpark Anomaly Detector)
CREATE TABLE IF NOT EXISTS COCO_DE_DEMO.SILVER.ANOMALY_FLAGS (
    ANOMALY_ID          VARCHAR DEFAULT UUID_STRING(),
    TABLE_NAME          VARCHAR,
    RECORD_ID           VARCHAR,
    ANOMALY_TYPE        VARCHAR,
    DETAILS             VARCHAR,
    DETECTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold: Customer RFM scores (populated by Snowpark Gold Enrichment)
CREATE TABLE IF NOT EXISTS COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES (
    CUSTOMER_ID         NUMBER,
    RECENCY_SCORE       NUMBER,
    FREQUENCY_SCORE     NUMBER,
    MONETARY_SCORE      NUMBER,
    RFM_SEGMENT         VARCHAR,
    SCORED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold: Pipeline run summary (populated by Snowpark Gold Enrichment)
CREATE TABLE IF NOT EXISTS COCO_DE_DEMO.GOLD.PIPELINE_RUN_SUMMARY (
    RUN_ID              VARCHAR DEFAULT UUID_STRING(),
    LAYER               VARCHAR,
    TABLE_NAME          VARCHAR,
    ROW_COUNT           NUMBER,
    RUN_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- PROCEDURE 1: Bronze Data Profiler (Python / Snowpark)
-- Profiles all 6 Bronze tables: row counts, null %, distinct counts
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.BRONZE.SNOWPARK_BRONZE_PROFILER()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session):
    from snowflake.snowpark.functions import col, count, lit, current_timestamp
    from datetime import datetime

    bronze_tables = ['CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PRODUCTS', 'PAYMENTS', 'SHIPMENTS']
    run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]
    profiled_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    total_metrics = 0

    for table_name in bronze_tables:
        fqn = f"COCO_DE_DEMO.BRONZE.{table_name}"
        df = session.table(fqn)

        # Row count
        row_count = df.count()
        session.sql(f"""
            INSERT INTO COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
                (PROFILE_RUN_ID, TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE, PROFILED_AT)
            VALUES ('{run_id}', '{table_name}', '*', 'ROW_COUNT', '{row_count}', '{profiled_at}')
        """).collect()
        total_metrics += 1

        # Per-column profiling
        schema_fields = df.schema.fields
        for field in schema_fields:
            col_name = field.name

            # Null count
            null_count = df.filter(col(col_name).is_null()).count()
            null_pct = round((null_count / row_count * 100), 2) if row_count > 0 else 0.0

            session.sql(f"""
                INSERT INTO COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
                    (PROFILE_RUN_ID, TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE, PROFILED_AT)
                VALUES ('{run_id}', '{table_name}', '{col_name}', 'NULL_COUNT', '{null_count}', '{profiled_at}')
            """).collect()

            session.sql(f"""
                INSERT INTO COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
                    (PROFILE_RUN_ID, TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE, PROFILED_AT)
                VALUES ('{run_id}', '{table_name}', '{col_name}', 'NULL_PCT', '{null_pct}', '{profiled_at}')
            """).collect()

            # Distinct count
            distinct_count = df.select(col(col_name)).distinct().count()
            session.sql(f"""
                INSERT INTO COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
                    (PROFILE_RUN_ID, TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE, PROFILED_AT)
                VALUES ('{run_id}', '{table_name}', '{col_name}', 'DISTINCT_COUNT', '{distinct_count}', '{profiled_at}')
            """).collect()

            total_metrics += 3

    return f"Bronze profiling complete: {total_metrics} metrics recorded across {len(bronze_tables)} tables (run_id={run_id})"
$$;


-- ============================================================
-- PROCEDURE 2: Silver Anomaly Detector (Python / Snowpark)
-- Detects statistical outliers and business anomalies in Silver data
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.SILVER.SNOWPARK_ANOMALY_DETECTOR()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session):
    from datetime import datetime

    detected_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # --- Anomaly 1: Order amount outliers (IQR method) ---
    stats = session.sql("""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TOTAL_AMOUNT) AS Q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TOTAL_AMOUNT) AS Q3
        FROM COCO_DE_DEMO.SILVER.STG_ORDERS
    """).collect()[0]

    q1 = float(stats['Q1'])
    q3 = float(stats['Q3'])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Use INSERT...SELECT for outlier orders (much faster than row-by-row)
    outlier_count = session.sql(f"""
        INSERT INTO COCO_DE_DEMO.SILVER.ANOMALY_FLAGS
            (TABLE_NAME, RECORD_ID, ANOMALY_TYPE, DETAILS, DETECTED_AT)
        SELECT
            'STG_ORDERS',
            TO_VARCHAR(ORDER_ID),
            'AMOUNT_OUTLIER',
            'Amount $' || TO_VARCHAR(TOTAL_AMOUNT) || ' outside IQR bounds [${lower_bound:.2f}, ${upper_bound:.2f}]',
            '{detected_at}'
        FROM COCO_DE_DEMO.SILVER.STG_ORDERS
        WHERE TOTAL_AMOUNT < {lower_bound} OR TOTAL_AMOUNT > {upper_bound}
    """).collect()[0][0]

    # --- Anomaly 2: Excessive discount percentages (>50%) ---
    high_discount_count = session.sql(f"""
        INSERT INTO COCO_DE_DEMO.SILVER.ANOMALY_FLAGS
            (TABLE_NAME, RECORD_ID, ANOMALY_TYPE, DETAILS, DETECTED_AT)
        SELECT
            'STG_ORDER_ITEMS',
            TO_VARCHAR(ORDER_ITEM_ID),
            'HIGH_DISCOUNT',
            'Discount ' || TO_VARCHAR(DISCOUNT_PCT) || '% exceeds 50% threshold (order_item_id=' || TO_VARCHAR(ORDER_ITEM_ID) || ')',
            '{detected_at}'
        FROM COCO_DE_DEMO.SILVER.STG_ORDER_ITEMS
        WHERE DISCOUNT_PCT > 50
    """).collect()[0][0]

    # --- Anomaly 3: Dormant customers (>365 day tenure, no orders in last 90 days) ---
    dormant_count = session.sql(f"""
        INSERT INTO COCO_DE_DEMO.SILVER.ANOMALY_FLAGS
            (TABLE_NAME, RECORD_ID, ANOMALY_TYPE, DETAILS, DETECTED_AT)
        SELECT
            'STG_CUSTOMERS',
            TO_VARCHAR(c.CUSTOMER_ID),
            'DORMANT_CUSTOMER',
            'Tenure >365 days with no orders in last 90 days',
            '{detected_at}'
        FROM COCO_DE_DEMO.SILVER.STG_CUSTOMERS c
        LEFT JOIN (
            SELECT CUSTOMER_ID, MAX(ORDER_DATE) AS LAST_ORDER
            FROM COCO_DE_DEMO.SILVER.STG_ORDERS
            GROUP BY CUSTOMER_ID
        ) o ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE c.CUSTOMER_TENURE_DAYS > 365
          AND (o.LAST_ORDER IS NULL OR DATEDIFF('day', o.LAST_ORDER, CURRENT_DATE()) > 90)
    """).collect()[0][0]

    total = outlier_count + high_discount_count + dormant_count
    return f"Anomaly detection complete: {total} anomalies flagged (outliers: {outlier_count}, high_discount: {high_discount_count}, dormant: {dormant_count})"
$$;


-- ============================================================
-- PROCEDURE 3: Gold Enrichment & Summary (Python / Snowpark)
-- RFM scoring + pipeline run summary
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.GOLD.SNOWPARK_GOLD_ENRICHMENT()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session):
    from datetime import datetime

    scored_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]

    # ---- Part A: Customer RFM Scoring ----
    # Pull customer metrics from dim_customers
    rfm_data = session.sql("""
        SELECT CUSTOMER_ID, DAYS_SINCE_LAST_ORDER, TOTAL_ORDERS, LIFETIME_SPEND
        FROM COCO_DE_DEMO.GOLD.DIM_CUSTOMERS
        WHERE TOTAL_ORDERS > 0
    """).collect()

    if len(rfm_data) > 0:
        recency_vals = sorted([float(r['DAYS_SINCE_LAST_ORDER']) for r in rfm_data if r['DAYS_SINCE_LAST_ORDER'] is not None])
        frequency_vals = sorted([float(r['TOTAL_ORDERS']) for r in rfm_data])
        monetary_vals = sorted([float(r['LIFETIME_SPEND']) for r in rfm_data])

        def quartile_score(val, sorted_vals, reverse=False):
            """Assign 1-4 score based on quartile position."""
            n = len(sorted_vals)
            if n == 0:
                return 2
            q1 = sorted_vals[int(n * 0.25)]
            q2 = sorted_vals[int(n * 0.50)]
            q3 = sorted_vals[int(n * 0.75)]
            if reverse:  # Lower = better (e.g., recency)
                if val <= q1: return 4
                elif val <= q2: return 3
                elif val <= q3: return 2
                else: return 1
            else:  # Higher = better (e.g., frequency, monetary)
                if val <= q1: return 1
                elif val <= q2: return 2
                elif val <= q3: return 3
                else: return 4

        # Truncate and re-insert RFM scores
        session.sql("DELETE FROM COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES").collect()

        batch_values = []
        for row in rfm_data:
            cust_id = row['CUSTOMER_ID']
            recency = float(row['DAYS_SINCE_LAST_ORDER']) if row['DAYS_SINCE_LAST_ORDER'] is not None else 9999
            frequency = float(row['TOTAL_ORDERS'])
            monetary = float(row['LIFETIME_SPEND'])

            r_score = quartile_score(recency, recency_vals, reverse=True)
            f_score = quartile_score(frequency, frequency_vals, reverse=False)
            m_score = quartile_score(monetary, monetary_vals, reverse=False)

            # RFM segment classification
            avg_score = (r_score + f_score + m_score) / 3.0
            if avg_score >= 3.5:
                segment = 'CHAMPION'
            elif avg_score >= 2.5:
                segment = 'LOYAL'
            elif avg_score >= 1.5:
                segment = 'AT_RISK'
            else:
                segment = 'HIBERNATING'

            batch_values.append(
                f"({cust_id}, {r_score}, {f_score}, {m_score}, '{segment}', '{scored_at}')"
            )

            # Insert in batches of 500
            if len(batch_values) >= 500:
                values_str = ", ".join(batch_values)
                session.sql(f"""
                    INSERT INTO COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES
                        (CUSTOMER_ID, RECENCY_SCORE, FREQUENCY_SCORE, MONETARY_SCORE, RFM_SEGMENT, SCORED_AT)
                    VALUES {values_str}
                """).collect()
                batch_values = []

        # Insert remaining
        if batch_values:
            values_str = ", ".join(batch_values)
            session.sql(f"""
                INSERT INTO COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES
                    (CUSTOMER_ID, RECENCY_SCORE, FREQUENCY_SCORE, MONETARY_SCORE, RFM_SEGMENT, SCORED_AT)
                VALUES {values_str}
            """).collect()

    rfm_count = session.sql("SELECT COUNT(*) AS C FROM COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES").collect()[0]['C']

    # ---- Part B: Pipeline Run Summary ----
    layer_tables = {
        'BRONZE': ['CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PRODUCTS', 'PAYMENTS', 'SHIPMENTS'],
        'SILVER': ['STG_CUSTOMERS', 'STG_ORDERS', 'STG_ORDER_ITEMS', 'STG_PRODUCTS', 'STG_PAYMENTS', 'STG_SHIPMENTS'],
        'GOLD': ['DIM_CUSTOMERS', 'DIM_PRODUCTS', 'DIM_DATES', 'FACT_SALES',
                 'FACT_DAILY_REVENUE', 'FACT_PAYMENT_SUMMARY', 'FACT_SHIPMENT_PERFORMANCE',
                 'CUSTOMER_RFM_SCORES'],
    }

    summary_values = []
    for layer, tables in layer_tables.items():
        for table in tables:
            fqn = f"COCO_DE_DEMO.{layer}.{table}"
            try:
                cnt = session.table(fqn).count()
            except Exception:
                cnt = -1
            summary_values.append(
                f"('{run_id}', '{layer}', '{table}', {cnt}, '{scored_at}')"
            )

    if summary_values:
        values_str = ", ".join(summary_values)
        session.sql(f"""
            INSERT INTO COCO_DE_DEMO.GOLD.PIPELINE_RUN_SUMMARY
                (RUN_ID, LAYER, TABLE_NAME, ROW_COUNT, RUN_TIMESTAMP)
            VALUES {values_str}
        """).collect()

    return f"Gold enrichment complete: {rfm_count} RFM scores computed, pipeline summary recorded (run_id={run_id})"
$$;
