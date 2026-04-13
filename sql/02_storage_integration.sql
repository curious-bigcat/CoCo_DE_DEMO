-- ============================================================
-- Phase 2: S3 Storage Integration & External Stage
-- Project: COCO_DE_DEMO (Medallion Architecture Pipeline)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE COCO_DE_DEMO;

-- 2.1 Create Storage Integration (secure S3 access without managing credentials)
CREATE OR REPLACE STORAGE INTEGRATION coco_demo_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/coco-d4bdemo-role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://coco-d4bdemo-de/assets/')
  COMMENT = 'S3 integration for COCO DE Demo - raw CSV ingestion';

-- 2.2 Retrieve Snowflake IAM User ARN and External ID
-- (Use these values to configure the AWS IAM trust policy on coco-d4bdemo-role)
DESCRIBE INTEGRATION coco_demo_s3_integration;

-- 2.3 Create External Stage pointing to S3 bucket
CREATE OR REPLACE STAGE bronze.raw_data_stage
  STORAGE_INTEGRATION = coco_demo_s3_integration
  URL = 's3://coco-d4bdemo-de/assets/'
  FILE_FORMAT = COCO_DE_DEMO.BRONZE.CSV_FORMAT
  COMMENT = 'External stage for raw CSV files on S3';

-- 2.4 Verify stage (list files - will work once IAM trust policy is configured)
-- LIST @bronze.raw_data_stage;
