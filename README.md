# COCO_DE_DEMO — End-to-End Data Engineering Pipeline with Cortex Code

A fully automated **medallion architecture** data pipeline built entirely using **Cortex Code** (Snowflake's AI coding CLI). This project demonstrates how to go from raw CSV files on S3 to analytics-ready Gold tables, complete with quality gates, governance, anomaly detection, and live dashboards — all orchestrated in Snowflake.

---

## What This Project Builds

```
S3 (CSV files)
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│  BRONZE (Raw Ingestion)                                 │
│  6 tables ← Snowpipe auto-ingest ← CDC Streams         │
└─────────────┬───────────────────────────────────────────┘
              │  Gate 1: null keys, empty tables, dupes
              ▼
┌─────────────────────────────────────────────────────────┐
│  SILVER (Cleaned & Validated)                           │
│  6 staging models (dbt) + anomaly detection (Snowpark)  │
└─────────────┬───────────────────────────────────────────┘
              │  Gate 2: referential integrity, negatives
              ▼
┌─────────────────────────────────────────────────────────┐
│  GOLD (Analytics-Ready)                                 │
│  7 dimension/fact models (dbt) + RFM scoring (Snowpark) │
└─────────────┬───────────────────────────────────────────┘
              │  Gate 3: row counts, valid tiers, null keys
              ▼
        Streamlit Dashboards (2)
```

**Key components:**

| Layer | What's Built |
|-------|-------------|
| **Ingestion** | S3 storage integration, external stage, 6 Snowpipe auto-ingest pipes |
| **CDC** | 6 streams with initial row capture |
| **Transformations** | dbt project with 6 Silver staging + 7 Gold analytics models |
| **Quality Gates** | 3 stored procedures (Bronze→Silver→Gold validation checkpoints) |
| **Orchestration** | 8-task DAG, event-driven (triggers on new stream data every minute) |
| **Data Quality** | System DMFs + 2 custom DMFs on 15 tables (60-min schedule) |
| **Governance** | 5 tag types across all layers + PII column tagging |
| **Snowpark Python** | Bronze Profiler, Silver Anomaly Detector, Gold RFM Enrichment |
| **Dashboards** | Pipeline Dashboard (5 tabs) + Governance Dashboard (5 tabs) |

---

## Dataset

Six retail/e-commerce CSV files:

| File | Description |
|------|-------------|
| `customers.csv` | Customer profiles — name, email, phone, city, state, segment, signup date |
| `orders.csv` | Order headers — date, status, sales channel, total amount |
| `order_items.csv` | Line items — quantity, unit price, discount |
| `products.csv` | Product catalog — category, brand, list price, cost price, stock |
| `payments.csv` | Payment records — method, amount, status |
| `shipments.csv` | Shipment tracking — carrier, ship/delivery dates, status |

Source: `s3://coco-d4bdemo-de/assets/`

---

## Project Structure

```
CoCo_DE/
├── agents.md                  # The 13 prompts used to build this project
├── snowflake.yml              # Snow CLI config for deploying Streamlit apps
├── pyproject.toml             # Python dependencies
│
├── data/                      # Source CSV files (6 files)
│
├── sql/                       # Snowflake deployment scripts (executed in order)
│   ├── 01_foundation.sql          # Database, schemas, file format
│   ├── 02_storage_integration.sql # S3 integration + external stage
│   ├── 03_bronze_tables.sql       # 6 Bronze landing tables
│   ├── 04_snowpipe.sql            # 6 auto-ingest pipes
│   ├── 05_streams.sql             # 6 CDC streams
│   ├── 06_validation_gates.sql    # 3 quality gate procedures
│   ├── 07_task_dag.sql            # 8-task orchestration DAG
│   ├── 08_data_quality.sql        # DMFs + monitoring schedules
│   ├── 09_object_tagging.sql      # Governance tags
│   └── 10_snowpark_processing.sql # 3 Snowpark Python procedures
│
├── dbt_project/               # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── silver/            # 6 staging models + schema.yml
│       └── gold/              # 7 analytics models + schema.yml
│
├── streamlit_app.py           # Pipeline Dashboard (Revenue, Customers, Products, Ops, Health)
└── governance_dashboard.py    # Governance Dashboard (Overview, DMFs, PII, Anomalies, Lineage)
```

---

## Prerequisites

- A Snowflake account with `ACCOUNTADMIN` role (or equivalent privileges)
- [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) installed and authenticated
- An active Snowflake connection configured in Cortex Code
- AWS S3 bucket access (for the storage integration step)

---

## How This Project Was Built — Using Cortex Code

This entire project was built by feeding **13 sequential prompts** to Cortex Code. Each prompt asked Cortex Code to create a specific layer of the pipeline. Cortex Code generated and executed all the SQL, dbt models, Python procedures, and Streamlit apps.

### Step-by-Step: The 13 Prompts

Open Cortex Code in your terminal and paste each prompt in order. Wait for each step to complete before moving to the next.

#### Prompt 1 — Foundation
> Create a Snowflake database called COCO_DE_DEMO with three schemas for a medallion architecture: BRONZE for raw data, SILVER for cleaned and validated data, and GOLD for analytics-ready data. Also create a reusable CSV file format in the Bronze schema that handles headers, quoted fields, and common null representations.

**What Cortex Code does:** Generates and executes `CREATE DATABASE`, `CREATE SCHEMA`, and `CREATE FILE FORMAT` statements. Output: `sql/01_foundation.sql`.

#### Prompt 2 — S3 Integration
> Create a storage integration for secure access to our S3 bucket at s3://coco-d4bdemo-de/assets/ using the IAM role arn:aws:iam::484577546576:role/coco-d4bdemo-role. Then create an external stage in the Bronze schema that points to this bucket and uses the CSV file format we created. After creating the integration, show me the Snowflake IAM user ARN and external ID so I can configure the AWS trust policy.

**What Cortex Code does:** Creates the storage integration and external stage, then retrieves the IAM ARN and external ID you need to update the AWS trust policy. Output: `sql/02_storage_integration.sql`.

> **Manual step:** Update the AWS IAM trust policy with the ARN and external ID that Cortex Code returns before proceeding.

#### Prompt 3 — Bronze Landing Tables
> Create 6 Bronze tables to receive the raw CSV data...

**What Cortex Code does:** Creates all 6 tables with appropriate column types. Output: `sql/03_bronze_tables.sql`.

#### Prompt 4 — Snowpipe Auto-Ingest
> Create Snowpipe auto-ingest pipes for all 6 Bronze tables. Use pattern matching instead of exact file paths...

**What Cortex Code does:** Creates 6 pipes with `PATTERN` clauses (e.g., `.*customers.*\\.csv`) and runs `ALTER PIPE ... REFRESH` to load existing files. Output: `sql/04_snowpipe.sql`.

> **Manual step:** Configure the S3 event notification to send to the SQS queue ARN that Cortex Code shows for each pipe.

#### Prompt 5 — CDC Streams
> Create CDC streams on all 6 Bronze tables. Enable them to capture existing rows...

**What Cortex Code does:** Creates 6 streams with `SHOW_INITIAL_ROWS = TRUE`. Output: `sql/05_streams.sql`.

#### Prompt 6 — dbt Transformations
> Generate a complete dbt project with two layers of models...

**What Cortex Code does:** Generates the entire `dbt_project/` directory — project config, profiles, macros, 6 Silver staging models, 7 Gold analytics models, and comprehensive `schema.yml` test files.

#### Prompt 7 — Deploy dbt to Snowflake
> Deploy the dbt project to Snowflake as a native dbt project object and execute it...

**What Cortex Code does:** Uses `snow dbt deploy` and `EXECUTE DBT PROJECT` to create all Silver and Gold tables in Snowflake.

#### Prompt 8 — Validation Gates
> Create 3 validation gate stored procedures...

**What Cortex Code does:** Creates 3 stored procedures that act as quality checkpoints between layers. Output: `sql/06_validation_gates.sql`.

#### Prompt 9 — Task DAG Orchestration
> Create an 8-task DAG that orchestrates the entire pipeline end-to-end...

**What Cortex Code does:** Creates an event-driven task graph that checks streams every minute and runs the full pipeline (Gate 1 → Profile → dbt Silver → Anomaly Detection → Gate 2 → dbt Gold → Enrichment → Gate 3). Output: `sql/07_task_dag.sql`.

#### Prompt 10 — Data Quality Monitoring
> Set up continuous data quality monitoring using Data Metric Functions...

**What Cortex Code does:** Attaches system DMFs (NULL_COUNT, DUPLICATE_COUNT, ROW_COUNT, FRESHNESS) and 2 custom DMFs to 15 tables on a 60-minute schedule. Output: `sql/08_data_quality.sql`.

#### Prompt 11 — Governance and Tagging
> Create a governance framework with 5 tag types...

**What Cortex Code does:** Creates 5 tag types (PIPELINE_LAYER, DATA_CLASSIFICATION, PII, DATA_DOMAIN, QUALITY_TIER) and applies them to all tables and PII columns. Output: `sql/09_object_tagging.sql`.

#### Prompt 12 — Snowpark Python Processing
> Create 3 Snowpark Python stored procedures...

**What Cortex Code does:** Creates 3 Python procedures (Bronze Profiler, Silver Anomaly Detector, Gold RFM Enrichment) plus 4 supporting output tables. Output: `sql/10_snowpark_processing.sql`.

#### Prompt 13 — Streamlit Dashboard
> Create a Streamlit in Snowflake dashboard with 5 tabs...

**What Cortex Code does:** Generates `streamlit_app.py` with 5 tabs (Revenue & Sales, Customers, Products & Inventory, Operations, Pipeline Health) and deploys it to Snowflake using the Snow CLI.

---

## How to Deploy This Project from Scratch

If you want to recreate this project in your own Snowflake account, you have two options:

### Option A: Replay the Prompts (Recommended)

1. Start Cortex Code:
   ```bash
   cortex
   ```

2. Copy each prompt from `agents.md` into Cortex Code, one at a time, in order (Prompts 1–13).

3. After Prompt 2, update your AWS IAM trust policy with the returned ARN and external ID.

4. After Prompt 4, configure S3 event notifications for the SQS queue ARNs.

### Option B: Run the SQL Scripts Directly

1. Execute the SQL scripts in order:
   ```
   sql/01_foundation.sql
   sql/02_storage_integration.sql
   sql/03_bronze_tables.sql
   sql/04_snowpipe.sql
   sql/05_streams.sql
   sql/06_validation_gates.sql
   sql/07_task_dag.sql
   sql/08_data_quality.sql
   sql/09_object_tagging.sql
   sql/10_snowpark_processing.sql
   ```

2. Deploy the dbt project:
   ```bash
   snow dbt deploy --project-dir dbt_project/
   snow sql -q "EXECUTE DBT PROJECT COCO_DE_DEMO.PUBLIC.COCO_DE_PIPELINE;"
   ```

3. Deploy the Streamlit dashboards:
   ```bash
   snow streamlit deploy coco_de_dashboard
   snow streamlit deploy coco_de_governance
   ```

> **Note:** You will need to update `sql/02_storage_integration.sql` with your own S3 bucket and IAM role, and update `dbt_project/profiles.yml` with your Snowflake account and warehouse.

---

## Snowflake Objects Created

| Schema | Object Type | Count | Examples |
|--------|------------|-------|---------|
| BRONZE | Tables | 6+ | CUSTOMERS, ORDERS, ORDER_ITEMS, PRODUCTS, PAYMENTS, SHIPMENTS |
| BRONZE | Pipes | 6 | CUSTOMERS_PIPE, ORDERS_PIPE, ... |
| BRONZE | Streams | 6 | CUSTOMERS_STREAM, ORDERS_STREAM, ... |
| BRONZE | File Format | 1 | CSV_FORMAT |
| BRONZE | Stage | 1 | RAW_DATA_STAGE |
| SILVER | Tables | 6+ | STG_CUSTOMERS, STG_ORDERS, STG_ORDER_ITEMS, ... |
| SILVER | Tables | 1 | ANOMALY_FLAGS |
| GOLD | Tables | 7+ | DIM_CUSTOMERS, DIM_PRODUCTS, DIM_DATES, FACT_SALES, ... |
| GOLD | Tables | 2 | RFM_SCORES, PIPELINE_RUN_SUMMARY |
| GOVERNANCE | Tags | 5 | PIPELINE_LAYER, DATA_CLASSIFICATION, PII, DATA_DOMAIN, QUALITY_TIER |
| PUBLIC | Tasks | 8 | Pipeline DAG (event-driven) |
| PUBLIC | Procedures | 6+ | 3 validation gates + 3 Snowpark Python procedures |
| PUBLIC | DMFs | 2 | POSITIVE_AMOUNT_CHECK, FK_INTEGRITY_CHECK |
| PUBLIC | Streamlit | 2 | COCO_DE_PIPELINE_DASHBOARD, COCO_DE_GOVERNANCE_DASHBOARD |
| PUBLIC | dbt Project | 1 | COCO_DE_PIPELINE |

---

## Cortex Code Features Demonstrated

This project showcases the following Cortex Code capabilities:

- **SQL generation and execution** — All DDL and DML statements generated from natural language
- **dbt project scaffolding** — Full project structure, models, tests, and macros generated from a single prompt
- **dbt deployment to Snowflake** — Native `snow dbt deploy` and `EXECUTE DBT PROJECT` via the dbt-projects-on-snowflake skill
- **Snowpark Python procedures** — Complex Python stored procedures with statistical analysis (IQR, RFM scoring)
- **Streamlit app creation** — Multi-tab dashboards with charts, metrics, and caching generated and deployed
- **Data quality setup** — System and custom DMFs attached to tables with monitoring schedules
- **Governance automation** — Tag creation and bulk application across schemas and columns
- **Task DAG orchestration** — Multi-step event-driven pipelines with conditional execution
- **Storage integrations** — S3 integration setup with IAM role configuration guidance

---

## Configuration

### Snowflake Connection
The project targets:
- **Database:** `COCO_DE_DEMO`
- **Warehouse:** `DEMO_WH`
- **Role:** `ACCOUNTADMIN`

Update `dbt_project/profiles.yml` and `snowflake.yml` for your environment.

### S3 Integration
Update `sql/02_storage_integration.sql` with your:
- S3 bucket path
- IAM role ARN

---

## License

This is a demonstration project for Cortex Code capabilities.
