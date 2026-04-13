-- ============================================================
-- Phase 2b: Bronze Tables
-- Project: COCO_DE_DEMO (Medallion Architecture Pipeline)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE COCO_DE_DEMO;
USE SCHEMA BRONZE;

-- 3.1 Customers table
CREATE OR REPLACE TABLE bronze.customers (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  segment STRING,
  created_at TIMESTAMP
)
COMMENT = 'Raw customer data ingested from S3 via Snowpipe';

-- 3.2 Orders table
CREATE OR REPLACE TABLE bronze.orders (
  order_id INT,
  customer_id INT,
  order_date TIMESTAMP,
  status STRING,
  channel STRING,
  total_amount DECIMAL(12,2)
)
COMMENT = 'Raw order data ingested from S3 via Snowpipe';

-- 3.3 Order Items table
CREATE OR REPLACE TABLE bronze.order_items (
  order_item_id INT,
  order_id INT,
  product_id INT,
  quantity INT,
  unit_price DECIMAL(10,2),
  discount DECIMAL(10,2),
  line_total DECIMAL(12,2)
)
COMMENT = 'Raw order line items ingested from S3 via Snowpipe';

-- 3.4 Products table
CREATE OR REPLACE TABLE bronze.products (
  product_id INT,
  product_name STRING,
  category STRING,
  subcategory STRING,
  brand STRING,
  unit_price DECIMAL(10,2),
  cost_price DECIMAL(10,2),
  stock_quantity INT,
  created_at TIMESTAMP
)
COMMENT = 'Raw product catalog ingested from S3 via Snowpipe';

-- 3.5 Payments table
CREATE OR REPLACE TABLE bronze.payments (
  payment_id INT,
  order_id INT,
  payment_method STRING,
  amount DECIMAL(12,2),
  payment_date TIMESTAMP,
  status STRING
)
COMMENT = 'Raw payment transactions ingested from S3 via Snowpipe';

-- 3.6 Shipments table
CREATE OR REPLACE TABLE bronze.shipments (
  shipment_id INT,
  order_id INT,
  carrier STRING,
  tracking_number STRING,
  ship_date TIMESTAMP,
  delivery_date TIMESTAMP,
  status STRING
)
COMMENT = 'Raw shipment tracking data ingested from S3 via Snowpipe';

-- Verify
SHOW TABLES IN SCHEMA COCO_DE_DEMO.BRONZE;
