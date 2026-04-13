# I Built a Production Data Pipeline Using Only AI Prompts — Here's How I Did It

## 13 prompts. 60+ Snowflake objects. A complete medallion architecture. Zero lines of code written by hand.

---

I wanted to see if I could build a real data engineering pipeline — not a demo, not a tutorial example — using nothing but natural language. No copy-pasting from docs. No Stack Overflow. Just me describing what I wanted to an AI, and watching it build.

The tool was Cortex Code, Snowflake's AI-powered CLI. The result was a production-grade medallion architecture with automated ingestion, dbt transformations, quality gates, anomaly detection, governance tagging, and live dashboards. All from 13 prompts.

This article is not about the code that was generated. You can read that in the [repo](https://github.com/curious-bigcat/CoCo_DE_DEMO). This is about how I used Cortex Code — the prompting strategy, the workflow, the surprises, and the things I'd do differently.

---

## What I Set Out to Build

A full medallion architecture pipeline on Snowflake for a retail e-commerce dataset (6 CSV files on S3: customers, orders, order items, products, payments, shipments).

The end state I had in mind:

- S3 ingestion via Snowpipe with auto-ingest
- CDC streams feeding an event-driven task DAG
- dbt Silver/Gold transformations deployed as a native Snowflake object
- Validation gates between each layer that halt the pipeline on bad data
- Snowpark Python procedures for profiling, anomaly detection, and customer scoring
- Data Metric Functions monitoring 15 tables continuously
- Governance tags on every table and PII column
- Streamlit dashboards for analytics and pipeline health

I knew the architecture I wanted. I just didn't want to write the code.

---

## The Setup

I opened my terminal, started Cortex Code, and began typing prompts. That's it. No project scaffolding, no boilerplate, no config files to set up first. Cortex Code connects to your Snowflake account and executes directly.

The key insight I had early on: **treat each prompt as a self-contained unit of work.** Don't try to build everything in one go. Each prompt should produce a testable, verifiable result before you move to the next one.

I ended up with 13 prompts, executed sequentially. Each built on the output of the previous one. Here's how that went.

---

## Prompt 1–3: Foundation, S3, and Bronze Tables

**What I asked for:** A database with three schemas (Bronze/Silver/Gold), an S3 storage integration, and 6 landing tables.

**How I prompted:** I was specific about naming — `COCO_DE_DEMO` for the database, `BRONZE`/`SILVER`/`GOLD` for schemas. I described the CSV file format behavior I wanted (handle headers, quoted fields, null representations) rather than dictating the exact parameters. For the tables, I listed every column name and type because I knew what was in my CSVs.

**What I noticed:** Cortex Code added `COMMENT` clauses to every object. I hadn't asked for that. It also chose `EMPTY_FIELD_AS_NULL = TRUE` and `TRIM_SPACE = TRUE` on the file format — sensible defaults I might have forgotten. Small thing, but it showed the tool was thinking about operational details, not just syntax.

**The one manual step:** After creating the storage integration, Cortex Code ran `DESCRIBE INTEGRATION` and showed me the Snowflake IAM user ARN and external ID. I had to go to AWS and update the trust policy myself. There's no way around this — it's a cross-cloud handshake. I'd asked for this in the prompt ("show me the ARN and external ID so I can configure the AWS trust policy") and Cortex Code handled the Snowflake side cleanly.

---

## Prompt 4: Snowpipe — Where It Got Interesting

**What I asked for:** Auto-ingest pipes for all 6 tables, using pattern matching so versioned files (like `customers_v2.csv`) would also get picked up.

**What surprised me:** The regex pattern for the orders pipe. I have both `orders.csv` and `order_items.csv` in the same S3 path. A naive `.*orders.*` pattern would match both. Cortex Code generated `'.*orders[_v0-9]*[.]csv'` for orders and a separate, distinct pattern for order_items. It understood the naming collision risk without me flagging it.

It also added `ALTER PIPE ... REFRESH` statements at the end to do an initial load of files already sitting in S3. I hadn't asked for that. It's the kind of thing you forget on first setup and then wonder why your tables are empty.

**Takeaway:** When you describe the *intent* ("use pattern matching so versioned files get picked up"), Cortex Code makes design decisions. When you describe the *implementation* ("use this exact regex"), it just types what you said. Intent-based prompts produce better results.

---

## Prompt 5: CDC Streams — A Subtle but Critical Detail

**What I asked for:** CDC streams on all 6 Bronze tables, enabled to capture existing rows.

**Why this mattered:** I'd already loaded data via Snowpipe in prompt 4. Standard Snowflake streams only see *future* changes. If I'd just said "create streams on the Bronze tables," the initial load would be invisible to downstream tasks. The pipeline would deploy, sit there waiting for new data, and never process what was already loaded.

The phrase "capture existing rows (not just future changes)" in my prompt caused Cortex Code to set `SHOW_INITIAL_ROWS = TRUE`. This is the kind of detail that separates a working pipeline from a pipeline that passes all tests but processes nothing.

**Lesson learned:** The more you understand the Snowflake feature you're prompting for, the better you can phrase the edge cases. `SHOW_INITIAL_ROWS` is one of those flags where the default behavior is almost never what you want on initial setup. I knew to ask for it. Someone who didn't know the flag existed might have spent an hour debugging.

---

## Prompt 6: dbt — The Biggest Single Prompt

**What I asked for:** A complete dbt project with Silver staging models for all 6 tables and 7 Gold analytics models, including comprehensive tests.

This was the longest prompt I wrote. I described derived columns I wanted in Silver (customer tenure, email domain, high-value flags, margin analysis, delivery speed classification), and business logic I wanted in Gold (loyalty tiers, sales performance tiers, a fully denormalized sales fact table).

**What Cortex Code produced:** 13 dbt models, `schema.yml` files with unique/not_null/relationships/accepted_values tests, `dbt_project.yml`, `profiles.yml`, and a custom macro. An entire project directory, ready to deploy.

**What impressed me:** The Gold `dim_customers` model used CTEs to aggregate orders, payments, and channel preferences separately before joining them to the customer base. The loyalty tier logic used both order count AND lifetime spend thresholds — exactly what I'd described. The `fact_sales` model joined 5 Silver tables through pre-aggregated CTEs to avoid fan-out joins. These are patterns a senior data engineer would use. I described the *what*, and it picked the right *how*.

**What I'd do differently:** This prompt was almost too big. If one model had an issue, I'd have to re-prompt and hope it didn't break the others. In retrospect, I'd split this into two prompts — one for Silver, one for Gold — so I could verify each layer independently.

---

## Prompt 7: Deploy dbt to Snowflake

**What I asked for:** Deploy the dbt project as a native Snowflake object and run it.

**Why this matters to the workflow:** Cortex Code used `snow dbt deploy` to push the project into Snowflake as a first-class object. After that, any task or procedure can call `EXECUTE DBT PROJECT` to run transformations. This is what makes prompt 9 (the task DAG) possible — dbt becomes callable from within Snowflake's native orchestration. No external scheduler, no dbt Cloud, no cron job on a VM somewhere.

---

## Prompt 8: Validation Gates — My Favorite Pattern

**What I asked for:** 3 stored procedures that act as quality checkpoints between layers, each returning a clear pass/fail message.

**The pattern Cortex Code built:** Each gate procedure runs a series of checks (null keys, empty tables, duplicate PKs, referential integrity, negative amounts, invalid tiers) and accumulates failures into a counter and a details string. At the end, it returns either `'GATE N PASSED: ...'` or `'GATE N FAILED (X checks): ...'`.

**Why I love this pattern:** The return value is just a string. The downstream task reads it with `SYSTEM$GET_PREDECESSOR_RETURN_VALUE()` and does a simple `LIKE 'GATE 1 PASSED%'` check. If the gate failed, the transformation is skipped — not errored, *skipped*. The pipeline keeps running, but bad data doesn't promote. You get a clean audit trail of exactly what failed and why.

I described this pattern in the prompt by saying "Return a clear pass/fail message with details" and "Each gate should pass its result to the next task so downstream steps can decide whether to proceed." Cortex Code connected the dots and used Snowflake's task return value mechanism. I didn't mention `SYSTEM$SET_RETURN_VALUE` or `SYSTEM$GET_PREDECESSOR_RETURN_VALUE` — it chose those on its own.

---

## Prompt 9: The Task DAG — Tying It All Together

**What I asked for:** An 8-task DAG that orchestrates the entire pipeline, event-driven, triggered only when streams have new data.

**The prompt strategy:** I numbered the 8 tasks and described the dependency chain explicitly: "Root task validates Bronze data and only triggers when streams have new data... Run dbt Silver models but only if Gate 1 passed... Validate Silver data (Gate 2)... Run dbt Gold models but only if Gate 2 passed."

**What Cortex Code did that I didn't ask for:**
- Created wrapper procedures (`run_silver_if_gate_passed`, `run_gold_if_gate_passed`) that read the predecessor's return value and conditionally execute dbt. I'd described the conditional logic, but didn't specify how to implement it.
- Used `SYSTEM$STREAM_HAS_DATA()` across all 6 streams in the root task's `WHEN` clause — so the pipeline fires if *any* table gets new data, not just one specific stream.
- Generated the `ALTER TASK ... RESUME` statements in bottom-up order (leaf tasks first, root task last). This is a Snowflake requirement that's easy to get wrong. Cortex Code knew the rule.

**What I learned:** The event-driven pattern is powerful. The root task checks every minute, but only *executes* when streams have data. No data means no compute, no cost. This replaced what would normally be a cron-triggered Airflow DAG with a native Snowflake construct that costs nothing when idle.

---

## Prompt 10: Data Quality Monitoring

**What I asked for:** System and custom DMFs on all tables, evaluating every 60 minutes.

**The prompting approach:** I asked for two specific custom DMFs (positive amounts, FK integrity) and let Cortex Code decide which system DMFs to attach where. It chose `NULL_COUNT` on primary keys, `DUPLICATE_COUNT` on primary keys, `ROW_COUNT` on all tables, and `FRESHNESS` on all tables. Reasonable defaults.

**What this gave me:** Continuous monitoring that runs independently of the pipeline. Even if the task DAG isn't triggered, the DMFs still evaluate every hour and log results. This catches drift, unexpected nulls, and stale data whether or not the pipeline ran.

---

## Prompt 11: Governance — Tags Everywhere

**What I asked for:** 5 tag types applied to every table across all layers, with PII tags on specific columns.

**Why I prompted for PII tags across all layers:** A common mistake is tagging PII only in Bronze and forgetting that the same email address flows through Silver and Gold. When you eventually attach a masking policy to the PII tag, you want it to protect data everywhere. I made this explicit in the prompt: "tag specific columns that contain personally identifiable information... as TRUE across all layers where those columns exist."

Cortex Code generated `ALTER TABLE ... MODIFY COLUMN ... SET TAG` for every PII column in every schema. Tedious work, but exactly the kind of thing an AI should do.

---

## Prompt 12: Snowpark Python — The Iteration Point

**What I asked for:** 3 Python stored procedures: Bronze Profiler, Silver Anomaly Detector, Gold RFM Enrichment.

**Where I had to iterate:** The anomaly detector. My first version of this prompt didn't specify how the data should be inserted. Cortex Code defaulted to row-by-row Python inserts — collect results into a list, loop through, insert each row. This works but is painfully slow on any real data volume.

I refined the prompt with: "Use bulk SQL operations for performance, not row-by-row Python inserts." The second version used `INSERT...SELECT` statements via `session.sql()`, pushing the heavy lifting to Snowflake's engine. Same result, orders of magnitude faster.

**This is the most important lesson from the whole project:** Cortex Code amplifies your engineering knowledge. If you know that `INSERT...SELECT` outperforms row-by-row inserts in Snowpark, you can ask for it. If you don't know, you get the naive version and might not realize there's a better way. The tool doesn't replace your understanding of the platform — it makes your understanding more productive.

**The RFM scoring** was interesting. I described the approach (quartile bucketing, segments like Champion/Loyal/At Risk/Hibernating) and Cortex Code implemented it with proper statistical quartile boundaries and a weighted average scoring function. It also added a pipeline run summary procedure that counts rows across every table in all three layers — which turned out to be exactly what I needed for the Pipeline Health tab in the dashboard.

---

## Prompt 13: Streamlit Dashboard — The Payoff

**What I asked for:** A 5-tab dashboard with specific charts, metrics, and caching.

**The prompt was very prescriptive:** I named each tab, listed which metrics to show as cards, and described each chart type (area chart for daily revenue, stacked bar for monthly breakdown, donut for channel distribution, etc.). For dashboards, being specific pays off. Vague prompts produce generic layouts.

**What Cortex Code added on its own:** Custom CSS for styled metric cards with colored left borders, Altair gradient fills on the area charts, and `@st.cache_data(ttl=timedelta(minutes=5))` on all 10 data-loading functions. The caching was in my prompt, but the CSS styling was a nice touch I didn't ask for.

The dashboard deploys natively to Snowflake via `snowflake.yml` — no external hosting needed.

---

## What I'd Tell Another Data Engineer

### 1. Prompt architecture, not implementation

The best prompts describe *what* you want the system to do, not *how* to code it. "Create validation gates that return pass/fail and halt downstream processing on failure" produces better results than "Write a stored procedure with a failures counter."

### 2. One prompt, one testable outcome

Each of my 13 prompts produced something I could verify immediately. After prompt 4, I could query the Bronze tables and see data. After prompt 8, I could call the gate procedures and see PASS/FAIL. This makes debugging trivial — if something breaks, you know exactly which prompt introduced the issue.

### 3. Specify edge cases you know about

The `SHOW_INITIAL_ROWS = TRUE` on streams, the regex pattern collision between orders and order_items, the bulk SQL vs. row-by-row performance — these are things I knew to ask for because I've built pipelines before. Cortex Code won't always catch these on its own. Your domain knowledge is the prompt's secret ingredient.

### 4. Iteration is normal, not failure

I refined the Snowpark prompt. I would have refined the dbt prompt if I did it again. This isn't a sign that the tool doesn't work — it's how you work with it. Think of each prompt as a PR. Review what it produced, refine if needed, move on.

### 5. The tool amplifies, it doesn't replace

Cortex Code generated 60+ Snowflake objects from 13 prompts. But those 13 prompts encode years of data engineering opinions: Bronze should never reject data, gate procedures should be fail-safe, PII tags should span all layers, event-driven beats cron-driven. The AI wrote the code. The architecture was mine.

---

## The Numbers

- **13 prompts** entered into Cortex Code
- **10 SQL scripts** generated (foundation through governance)
- **13 dbt models** (6 Silver staging, 7 Gold analytics)
- **3 validation gate procedures** with 10+ checks each
- **8-task event-driven DAG**
- **3 Snowpark Python procedures**
- **60+ total Snowflake objects**
- **2 Streamlit dashboards** with 10 tabs of analytics
- **0 lines of code written by hand**

---

## Try It Yourself

The full project is open source: [github.com/curious-bigcat/CoCo_DE_DEMO](https://github.com/curious-bigcat/CoCo_DE_DEMO)

**Option A — Replay the prompts (recommended):**

1. Clone the repo
2. Open `agents.md` — it has all 13 prompts in order
3. Start Cortex Code and paste each prompt sequentially
4. After Prompt 2, update your AWS IAM trust policy
5. After Prompt 4, configure S3 event notifications

**Option B — Run the scripts directly:**

1. Execute the 10 SQL scripts in `sql/` in order
2. Deploy dbt with `snow dbt deploy`
3. Deploy dashboards with `snow streamlit deploy`

Update the S3 bucket, IAM role, and Snowflake account details to match your environment.

---

## Final Thought

The future of data engineering isn't writing less code. It's describing better systems. The engineer who understands medallion architecture, CDC patterns, quality gates, and governance tagging will write better prompts than someone who doesn't — and get better pipelines as a result.

Cortex Code didn't make me a better data engineer. It made my data engineering faster.

---

*The full project is open source at [github.com/curious-bigcat/CoCo_DE_DEMO](https://github.com/curious-bigcat/CoCo_DE_DEMO). Star it if you found this useful.*

*Built with [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) by Snowflake.*
