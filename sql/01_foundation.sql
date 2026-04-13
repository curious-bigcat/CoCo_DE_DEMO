-- ============================================================
-- Phase 1: Foundation - Database, Schemas, File Format
-- Project: COCO_DE_DEMO (Medallion Architecture Pipeline)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;

-- 1.1 Create Database
CREATE OR REPLACE DATABASE COCO_DE_DEMO
  COMMENT = 'Cortex Code DE Demo - Medallion Architecture Pipeline';

-- 1.2 Create Medallion Schemas
CREATE OR REPLACE SCHEMA COCO_DE_DEMO.BRONZE
  COMMENT = 'Raw data layer - ingested from S3 via Snowpipe';

CREATE OR REPLACE SCHEMA COCO_DE_DEMO.SILVER
  COMMENT = 'Cleaned and standardized data layer';

CREATE OR REPLACE SCHEMA COCO_DE_DEMO.GOLD
  COMMENT = 'Aggregated business-ready data layer';

-- 1.3 Create Reusable CSV File Format
CREATE OR REPLACE FILE FORMAT COCO_DE_DEMO.BRONZE.CSV_FORMAT
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  COMMENT = 'Standard CSV format for raw data ingestion';

-- Verify
SHOW SCHEMAS IN DATABASE COCO_DE_DEMO;
SHOW FILE FORMATS IN SCHEMA COCO_DE_DEMO.BRONZE;
