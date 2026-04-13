-- ============================================================
-- 07: Task DAG Orchestration (8-task hybrid SQL + Python pipeline)
-- Pipeline: Root (Gate 1) -> Bronze Profile (Snowpark)
--           -> Silver dbt -> Anomaly Detect (Snowpark) -> Gate 2
--           -> Gold dbt -> Gold Enrich (Snowpark) -> Gate 3
-- ============================================================

-- Gating wrapper procedures
-- Silver: checks Gate 1 result before running dbt Silver models
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.PUBLIC.run_silver_if_gate_passed(predecessor_result VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  IF (:predecessor_result LIKE 'GATE 1 PASSED%') THEN
    EXECUTE DBT PROJECT COCO_DE_DEMO.PUBLIC.COCO_DE_PIPELINE
      ARGS = 'run --select stg_customers stg_orders stg_order_items stg_products stg_payments stg_shipments';
    RETURN 'Silver transformation completed successfully';
  ELSE
    RETURN 'Silver transformation SKIPPED: Gate 1 did not pass. Result: ' || :predecessor_result;
  END IF;
END;
$$;

-- Gold: checks Gate 2 result before running dbt Gold models
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.PUBLIC.run_gold_if_gate_passed(predecessor_result VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  IF (:predecessor_result LIKE 'GATE 2 PASSED%') THEN
    EXECUTE DBT PROJECT COCO_DE_DEMO.PUBLIC.COCO_DE_PIPELINE
      ARGS = 'run --select dim_customers dim_products fact_sales';
    RETURN 'Gold transformation completed successfully';
  ELSE
    RETURN 'Gold transformation SKIPPED: Gate 2 did not pass. Result: ' || :predecessor_result;
  END IF;
END;
$$;


-- Step 1: Root Task (checks every 1 min, only runs when streams have new data)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_PIPELINE_ROOT
  WAREHOUSE = DEMO_WH
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Root task: auto-triggers when any Bronze stream has new data, validates Bronze (Gate 1)'
  WHEN
    SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.CUSTOMERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.ORDERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.ORDER_ITEMS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.PRODUCTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.PAYMENTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('COCO_DE_DEMO.BRONZE.SHIPMENTS_STREAM')
AS
  BEGIN
    LET gate_result VARCHAR;
    CALL COCO_DE_DEMO.BRONZE.VALIDATE_BRONZE_GATE() INTO :gate_result;
    CALL SYSTEM$SET_RETURN_VALUE(:gate_result);
  END;

-- Step 2: Snowpark Bronze Profiler (Python — profiles all Bronze tables)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_BRONZE_PROFILE
  WAREHOUSE = DEMO_WH
  COMMENT = 'Snowpark Python: profiles all Bronze tables (row counts, null %, distinct counts)'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_PIPELINE_ROOT
AS
  CALL COCO_DE_DEMO.BRONZE.SNOWPARK_BRONZE_PROFILER();

-- Step 3: Silver Transformation (checks Gate 1 result, runs dbt Silver)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_SILVER_TRANSFORM
  WAREHOUSE = DEMO_WH
  COMMENT = 'Runs dbt Silver models if Gate 1 passed'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_BRONZE_PROFILE
AS
  BEGIN
    LET transform_result VARCHAR;
    CALL COCO_DE_DEMO.PUBLIC.run_silver_if_gate_passed(
      SYSTEM$GET_PREDECESSOR_RETURN_VALUE('TASK_PIPELINE_ROOT')
    ) INTO :transform_result;
    CALL SYSTEM$SET_RETURN_VALUE(:transform_result);
  END;

-- Step 4: Snowpark Anomaly Detector (Python — IQR outliers, high discounts, dormant customers)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_ANOMALY_DETECT
  WAREHOUSE = DEMO_WH
  COMMENT = 'Snowpark Python: detects statistical outliers and business anomalies in Silver data'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_SILVER_TRANSFORM
AS
  CALL COCO_DE_DEMO.SILVER.SNOWPARK_ANOMALY_DETECTOR();

-- Step 5: Silver Gate (Gate 2 - validates Silver before Gold promotion)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_SILVER_GATE
  WAREHOUSE = DEMO_WH
  COMMENT = 'Gate 2: validates Silver data quality'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_ANOMALY_DETECT
AS
  BEGIN
    LET gate_result VARCHAR;
    CALL COCO_DE_DEMO.SILVER.VALIDATE_SILVER_GATE() INTO :gate_result;
    CALL SYSTEM$SET_RETURN_VALUE(:gate_result);
  END;

-- Step 6: Gold Transformation (checks Gate 2 result, runs dbt Gold)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_GOLD_TRANSFORM
  WAREHOUSE = DEMO_WH
  COMMENT = 'Runs dbt Gold models if Gate 2 passed'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_SILVER_GATE
AS
  BEGIN
    LET transform_result VARCHAR;
    CALL COCO_DE_DEMO.PUBLIC.run_gold_if_gate_passed(
      SYSTEM$GET_PREDECESSOR_RETURN_VALUE('TASK_SILVER_GATE')
    ) INTO :transform_result;
    CALL SYSTEM$SET_RETURN_VALUE(:transform_result);
  END;

-- Step 7: Snowpark Gold Enrichment (Python — RFM scoring + pipeline summary)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_GOLD_ENRICH
  WAREHOUSE = DEMO_WH
  COMMENT = 'Snowpark Python: RFM scoring and pipeline run summary'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_GOLD_TRANSFORM
AS
  CALL COCO_DE_DEMO.GOLD.SNOWPARK_GOLD_ENRICHMENT();

-- Step 8: Gold Gate (Gate 3 - certifies Gold for analytics)
CREATE OR REPLACE TASK COCO_DE_DEMO.PUBLIC.TASK_GOLD_GATE
  WAREHOUSE = DEMO_WH
  COMMENT = 'Gate 3: certifies Gold data for analytics'
  AFTER COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_GOLD_ENRICH
AS
  BEGIN
    LET gate_result VARCHAR;
    CALL COCO_DE_DEMO.GOLD.VALIDATE_GOLD_GATE() INTO :gate_result;
    CALL SYSTEM$SET_RETURN_VALUE(:gate_result);
  END;

-- Step 9: Resume all tasks (bottom-up, then root last)
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_GOLD_GATE RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_GOLD_ENRICH RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_GOLD_TRANSFORM RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_SILVER_GATE RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_ANOMALY_DETECT RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_SILVER_TRANSFORM RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_SNOWPARK_BRONZE_PROFILE RESUME;
ALTER TASK COCO_DE_DEMO.PUBLIC.TASK_PIPELINE_ROOT RESUME;
