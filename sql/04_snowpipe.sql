-- ============================================================
-- Phase 2c: Snowpipe (AUTO_INGEST) for all Bronze tables
-- Project: COCO_DE_DEMO (Medallion Architecture Pipeline)
-- ============================================================
-- Snowpipe SQS ARN (configure this on S3 bucket event notifications):
-- arn:aws:sqs:us-west-2:014498645395:sf-snowpipe-AIDAQGYBPYWJWZSXXDPDY-3QltwttaPzjyy32GycoWSQ
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE COCO_DE_DEMO;

-- 4.1 Customers Pipe (pattern-based to accept versioned files like customers_v2.csv)
CREATE OR REPLACE PIPE bronze.customers_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.customers
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*customers.*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.2 Orders Pipe (pattern excludes order_items)
CREATE OR REPLACE PIPE bronze.orders_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.orders
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*orders[_v0-9]*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.3 Order Items Pipe
CREATE OR REPLACE PIPE bronze.order_items_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.order_items
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*order_items.*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.4 Products Pipe
CREATE OR REPLACE PIPE bronze.products_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.products
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*products.*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.5 Payments Pipe
CREATE OR REPLACE PIPE bronze.payments_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.payments
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*payments.*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.6 Shipments Pipe
CREATE OR REPLACE PIPE bronze.shipments_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO bronze.shipments
  FROM @bronze.raw_data_stage/raw_data_stage/
  PATTERN = '.*shipments.*[.]csv'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT;

-- 4.7 Refresh pipes to load existing files
ALTER PIPE bronze.customers_pipe REFRESH;
ALTER PIPE bronze.orders_pipe REFRESH;
ALTER PIPE bronze.order_items_pipe REFRESH;
ALTER PIPE bronze.products_pipe REFRESH;
ALTER PIPE bronze.payments_pipe REFRESH;
ALTER PIPE bronze.shipments_pipe REFRESH;

-- Verify
SHOW PIPES IN SCHEMA COCO_DE_DEMO.BRONZE;
