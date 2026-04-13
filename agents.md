# Build a Complete Data Engineering Pipeline with Cortex Code

Give Cortex Code the prompts below in order. Each prompt builds on the previous one. By the end, you'll have a fully automated medallion architecture pipeline with ingestion, transformation, quality gates, governance, anomaly detection, and a live dashboard — all in Snowflake.

---

## Dataset

We're working with a retail / e-commerce dataset stored as 6 CSV files in an S3 bucket:

- **customers** — customer profiles with name, email, phone, city, state, zip code, segment, and signup date
- **orders** — order headers with date, status, sales channel, and total amount
- **order_items** — line items linking orders to products with quantity, unit price, and discount
- **products** — product catalog with category, brand, list price, cost price, and stock quantity
- **payments** — payment records with method, amount, and payment status
- **shipments** — shipment tracking with carrier, ship date, delivery date, and status

The S3 bucket is `s3://coco-d4bdemo-de/assets/` and uses IAM role `arn:aws:iam::12345678:role/coco-d4bdemo-role`.


---

## Prompt 1: Foundation

Create a Snowflake database called COCO_DE_DEMO with three schemas for a medallion architecture: BRONZE for raw data, SILVER for cleaned and validated data, and GOLD for analytics-ready data. Also create a reusable CSV file format in the Bronze schema that handles headers, quoted fields, and common null representations.

---

## Prompt 2: S3 Integration

Create a storage integration for secure access to our S3 bucket at s3://coco-d4bdemo-de/assets/ using the IAM role arn:aws:iam::484577546576:role/coco-d4bdemo-role. Then create an external stage in the Bronze schema that points to this bucket and uses the CSV file format we created. After creating the integration, show me the Snowflake IAM user ARN and external ID so I can configure the AWS trust policy.

---

## Prompt 3: Bronze Landing Tables

Create 6 Bronze tables to receive the raw CSV data. The tables should match the CSV column structures: customers (with customer_id, first_name, last_name, email, phone, city, state, zip_code, segment, created_at), orders (order_id, customer_id, order_date, status, sales_channel, total_amount), order_items (order_item_id, order_id, product_id, quantity, unit_price, discount), products (product_id, product_name, category, brand, list_price, cost_price, stock_quantity), payments (payment_id, order_id, payment_method, amount, payment_date, status), and shipments (shipment_id, order_id, carrier, ship_date, delivery_date, status).

---

## Prompt 4: Automated Ingestion with Snowpipe

Create Snowpipe auto-ingest pipes for all 6 Bronze tables. Use pattern matching instead of exact file paths so the pipes accept versioned files — for example, both customers.csv and customers_v2.csv should be picked up by the customers pipe. After creating the pipes, refresh them to load any files already in the stage.

---

## Prompt 5: Change Data Capture Streams

Create CDC streams on all 6 Bronze tables. Enable them to capture existing rows (not just future changes) so the pipeline can process the initial data load.

---

## Prompt 6: dbt Transformations

Generate a complete dbt project with two layers of models:

**Silver staging models** for all 6 tables — clean nulls, standardize formats, and add useful derived columns. For example: customer tenure in days, full name, email domain; order day-of-week, days since order, high-value flags; line item discount percentage and margin analysis; product price tiers and margin percentages; payment method grouping and timing; shipment delivery days and shipping speed classification.

**Gold analytics models:**
- A customer dimension with loyalty tiers based on order count, lifetime spend, average order value, and cancellation rate
- A product dimension with sales performance tiers, stock analysis, and margin classification
- A date dimension covering the full date range in the data
- A sales fact table joining orders, line items, and products
- A daily revenue fact with breakdowns by sales channel and order status
- A payment summary fact with payment outcomes per order
- A shipment performance fact with delivery metrics

Include comprehensive tests in schema.yml — unique and not_null on all primary keys, relationship tests for all foreign keys, and accepted_values tests for status fields, tiers, and segments.

---

## Prompt 7: Deploy dbt to Snowflake

Deploy the dbt project to Snowflake as a native dbt project object and execute it to create all the Silver and Gold tables.

---

## Prompt 8: Validation Gates

Create 3 validation gate stored procedures that act as quality checkpoints between layers:

**Gate 1 (Bronze to Silver):** Check all 6 Bronze tables for null primary keys, empty tables, and duplicate primary keys. Return a clear pass/fail message with details.

**Gate 2 (Silver to Gold):** Check referential integrity — every order should have a valid customer, every line item should have a valid order and product, every payment should link to a real order. Also check for null required fields and negative amounts. Return pass/fail.

**Gate 3 (Gold certification):** Verify Gold tables have consistent row counts with their Silver sources, all Gold tables are populated, no null keys exist, and tier/classification values are all valid. Return pass/fail.

---

## Prompt 9: Task DAG Orchestration

Create an 8-task DAG that orchestrates the entire pipeline end-to-end. The pipeline should be event-driven — it should only run when new data arrives in the Bronze streams, checking every minute. The flow should be:

1. Root task validates Bronze data (Gate 1) and only triggers when streams have new data
2. Profile all Bronze tables using a Snowpark Python procedure
3. Run dbt Silver models (but only if Gate 1 passed)
4. Detect anomalies in the Silver data using a Snowpark Python procedure
5. Validate Silver data (Gate 2)
6. Run dbt Gold models (but only if Gate 2 passed)
7. Enrich Gold data with customer scoring using a Snowpark Python procedure
8. Certify Gold data (Gate 3)

Each gate should pass its result to the next task so downstream steps can decide whether to proceed.

---

## Prompt 10: Data Quality Monitoring

Set up continuous data quality monitoring using Data Metric Functions across all three layers. Attach system DMFs for null counts, duplicate counts, row counts, and freshness to key columns on all Bronze, Silver, and Gold tables. Create two custom DMFs: one that validates amounts are positive, and one that checks referential integrity between orders and customers. Set all monitored tables to evaluate on a 60-minute schedule.

---

## Prompt 11: Governance and Tagging

Create a governance framework with 5 tag types:

- **Pipeline Layer** — tag every table as BRONZE, SILVER, or GOLD
- **Data Classification** — tag tables as PUBLIC, INTERNAL, CONFIDENTIAL, or RESTRICTED
- **PII** — tag specific columns that contain personally identifiable information (customer email, phone, first name, last name) as TRUE across all layers where those columns exist
- **Data Domain** — tag tables by business domain: CUSTOMER, ORDER, PRODUCT, PAYMENT, or SHIPMENT
- **Quality Tier** — tag tables as RAW, VALIDATED, or CERTIFIED based on their layer

Apply these tags to all tables across Bronze, Silver, and Gold.

---

## Prompt 12: Snowpark Python Processing

Create 3 Snowpark Python stored procedures with their supporting output tables:

**Bronze Profiler** — profile all 6 Bronze tables by computing row counts, null counts and percentages, and distinct value counts for every column. Store results in a profiling log table.

**Silver Anomaly Detector** — detect three types of anomalies: order amount outliers using the IQR statistical method, line items with unusually high discounts (over 50%), and dormant customers (signed up over a year ago but no orders in the last 90 days). Store all flagged anomalies in an anomaly flags table. Use bulk SQL operations for performance, not row-by-row Python inserts.

**Gold Enrichment** — compute RFM (Recency, Frequency, Monetary) scores for every customer using quartile bucketing, then assign segments like Champion, Loyal, At Risk, and Hibernating based on the scores. Also create a pipeline run summary that records row counts across every table in all three layers. Store results in an RFM scores table and a pipeline summary table.

---

## Prompt 13: Streamlit Dashboard

Create a Streamlit in Snowflake dashboard with 5 tabs:

**Tab 1 — Revenue & Sales:** Show top-line metrics (total revenue, total orders, unique customers, average order value) as styled metric cards. Include a daily gross revenue area chart, a monthly revenue by order status stacked bar chart, an orders by sales channel donut chart, and a daily orders vs customers trend line chart with a legend.

**Tab 2 — Customers:** Show customer metrics (total customers, average lifetime spend, average order value). Include a total spend by segment bar chart, a loyalty tier distribution donut chart, and a detailed segment breakdown table.

**Tab 3 — Products & Inventory:** Show product metrics (total products, average price, low stock count). Include a top 15 products table, a revenue by category horizontal bar chart, and a low stock alerts table for products below 10 units.

**Tab 4 — Operations:** Show operational metrics (total shipments, on-time delivery rate, average delivery days, payment success rate). Include a shipments by carrier bar chart, a shipping speed donut chart, payment health metrics, and a payment outcome bar chart.

**Tab 5 — Pipeline Health:** Show the RFM segment distribution as a bar chart, an anomaly detection summary with counts by type, pipeline row counts by layer as a bar chart colored by Bronze/Silver/Gold, and the Bronze data profile table.

Use caching with a 5-minute refresh interval. All charts should have labeled axes and legends. Deploy the dashboard natively in Snowflake.

