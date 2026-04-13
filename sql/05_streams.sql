-- ============================================================
-- Phase 3: Streams (CDC) on all Bronze tables
-- Project: COCO_DE_DEMO (Medallion Architecture Pipeline)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE COCO_DE_DEMO;

-- 5.1 Customers Stream
CREATE OR REPLACE STREAM bronze.customers_stream
  ON TABLE bronze.customers
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze customers for Silver processing';

-- 5.2 Orders Stream
CREATE OR REPLACE STREAM bronze.orders_stream
  ON TABLE bronze.orders
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze orders for Silver processing';

-- 5.3 Order Items Stream
CREATE OR REPLACE STREAM bronze.order_items_stream
  ON TABLE bronze.order_items
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze order_items for Silver processing';

-- 5.4 Products Stream
CREATE OR REPLACE STREAM bronze.products_stream
  ON TABLE bronze.products
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze products for Silver processing';

-- 5.5 Payments Stream
CREATE OR REPLACE STREAM bronze.payments_stream
  ON TABLE bronze.payments
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze payments for Silver processing';

-- 5.6 Shipments Stream
CREATE OR REPLACE STREAM bronze.shipments_stream
  ON TABLE bronze.shipments
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream on Bronze shipments for Silver processing';

-- Verify
SHOW STREAMS IN SCHEMA COCO_DE_DEMO.BRONZE;
