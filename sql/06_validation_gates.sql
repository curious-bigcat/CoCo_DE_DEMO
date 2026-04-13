-- ============================================================
-- 06: Validation Gate Procedures
-- Bronze -> Silver -> Gold pipeline quality gates
-- ============================================================

-- ============================================================
-- GATE 1: Bronze Validation (Bronze -> Silver)
-- Checks: null keys, empty tables, duplicate PKs
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.BRONZE.validate_bronze_gate()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  failures NUMBER DEFAULT 0;
  details VARCHAR DEFAULT '';
  null_order_keys NUMBER;
  null_cust_keys NUMBER;
  cust_cnt NUMBER;
  ord_cnt NUMBER;
  oi_cnt NUMBER;
  prod_cnt NUMBER;
  pay_cnt NUMBER;
  ship_cnt NUMBER;
  dup_orders NUMBER;
  dup_customers NUMBER;
  dup_products NUMBER;
BEGIN
  SELECT COUNT(*) INTO :null_order_keys FROM COCO_DE_DEMO.BRONZE.orders
  WHERE order_id IS NULL OR customer_id IS NULL;
  IF (:null_order_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_order_keys || ' null key values in orders. ';
  END IF;

  SELECT COUNT(*) INTO :null_cust_keys FROM COCO_DE_DEMO.BRONZE.customers
  WHERE customer_id IS NULL OR email IS NULL;
  IF (:null_cust_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_cust_keys || ' null key values in customers. ';
  END IF;

  SELECT COUNT(*) INTO :cust_cnt FROM COCO_DE_DEMO.BRONZE.customers;
  SELECT COUNT(*) INTO :ord_cnt FROM COCO_DE_DEMO.BRONZE.orders;
  SELECT COUNT(*) INTO :oi_cnt FROM COCO_DE_DEMO.BRONZE.order_items;
  SELECT COUNT(*) INTO :prod_cnt FROM COCO_DE_DEMO.BRONZE.products;
  SELECT COUNT(*) INTO :pay_cnt FROM COCO_DE_DEMO.BRONZE.payments;
  SELECT COUNT(*) INTO :ship_cnt FROM COCO_DE_DEMO.BRONZE.shipments;

  IF (:cust_cnt = 0 OR :ord_cnt = 0 OR :oi_cnt = 0 OR :prod_cnt = 0 OR :pay_cnt = 0 OR :ship_cnt = 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: One or more Bronze tables are empty (cust=' || :cust_cnt
      || ', ord=' || :ord_cnt || ', oi=' || :oi_cnt || ', prod=' || :prod_cnt
      || ', pay=' || :pay_cnt || ', ship=' || :ship_cnt || '). ';
  END IF;

  SELECT COUNT(*) INTO :dup_orders FROM (
    SELECT order_id FROM COCO_DE_DEMO.BRONZE.orders GROUP BY order_id HAVING COUNT(*) > 1
  );
  IF (:dup_orders > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :dup_orders || ' duplicate order_ids. ';
  END IF;

  SELECT COUNT(*) INTO :dup_customers FROM (
    SELECT customer_id FROM COCO_DE_DEMO.BRONZE.customers GROUP BY customer_id HAVING COUNT(*) > 1
  );
  IF (:dup_customers > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :dup_customers || ' duplicate customer_ids. ';
  END IF;

  SELECT COUNT(*) INTO :dup_products FROM (
    SELECT product_id FROM COCO_DE_DEMO.BRONZE.products GROUP BY product_id HAVING COUNT(*) > 1
  );
  IF (:dup_products > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :dup_products || ' duplicate product_ids. ';
  END IF;

  IF (:failures > 0) THEN
    RETURN 'GATE 1 FAILED (' || :failures || ' checks): ' || :details;
  ELSE
    RETURN 'GATE 1 PASSED: Bronze data validated for Silver promotion';
  END IF;
END;
$$;


-- ============================================================
-- GATE 2: Silver Validation (Silver -> Gold)
-- Checks: referential integrity, null required fields, value ranges
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.SILVER.validate_silver_gate()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  failures NUMBER DEFAULT 0;
  details VARCHAR DEFAULT '';
  orphan_orders NUMBER;
  orphan_items NUMBER;
  orphan_payments NUMBER;
  null_orders NUMBER;
  neg_amounts NUMBER;
  stg_cust NUMBER;
  stg_ord NUMBER;
  stg_oi NUMBER;
  stg_prod NUMBER;
  stg_pay NUMBER;
  stg_ship NUMBER;
BEGIN
  SELECT COUNT(*) INTO :orphan_orders FROM COCO_DE_DEMO.SILVER.stg_orders o
  WHERE NOT EXISTS (
    SELECT 1 FROM COCO_DE_DEMO.SILVER.stg_customers c WHERE c.customer_id = o.customer_id
  );
  IF (:orphan_orders > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :orphan_orders || ' orphan orders (no matching customer). ';
  END IF;

  SELECT COUNT(*) INTO :orphan_items FROM COCO_DE_DEMO.SILVER.stg_order_items oi
  WHERE NOT EXISTS (
    SELECT 1 FROM COCO_DE_DEMO.SILVER.stg_orders o WHERE o.order_id = oi.order_id
  );
  IF (:orphan_items > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :orphan_items || ' orphan order_items (no matching order). ';
  END IF;

  SELECT COUNT(*) INTO :orphan_payments FROM COCO_DE_DEMO.SILVER.stg_payments p
  WHERE NOT EXISTS (
    SELECT 1 FROM COCO_DE_DEMO.SILVER.stg_orders o WHERE o.order_id = p.order_id
  );
  IF (:orphan_payments > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :orphan_payments || ' orphan payments (no matching order). ';
  END IF;

  SELECT COUNT(*) INTO :null_orders FROM COCO_DE_DEMO.SILVER.stg_orders
  WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL;
  IF (:null_orders > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_orders || ' null required fields in stg_orders. ';
  END IF;

  SELECT COUNT(*) INTO :neg_amounts FROM COCO_DE_DEMO.SILVER.stg_orders WHERE total_amount < 0;
  IF (:neg_amounts > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :neg_amounts || ' negative amounts in stg_orders. ';
  END IF;

  SELECT COUNT(*) INTO :stg_cust FROM COCO_DE_DEMO.SILVER.stg_customers;
  SELECT COUNT(*) INTO :stg_ord FROM COCO_DE_DEMO.SILVER.stg_orders;
  SELECT COUNT(*) INTO :stg_oi FROM COCO_DE_DEMO.SILVER.stg_order_items;
  SELECT COUNT(*) INTO :stg_prod FROM COCO_DE_DEMO.SILVER.stg_products;
  SELECT COUNT(*) INTO :stg_pay FROM COCO_DE_DEMO.SILVER.stg_payments;
  SELECT COUNT(*) INTO :stg_ship FROM COCO_DE_DEMO.SILVER.stg_shipments;

  IF (:stg_cust = 0 OR :stg_ord = 0 OR :stg_oi = 0 OR :stg_prod = 0 OR :stg_pay = 0 OR :stg_ship = 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: One or more Silver tables are empty. ';
  END IF;

  IF (:failures > 0) THEN
    RETURN 'GATE 2 FAILED (' || :failures || ' checks): ' || :details;
  ELSE
    RETURN 'GATE 2 PASSED: Silver data validated for Gold promotion';
  END IF;
END;
$$;


-- ============================================================
-- GATE 3: Gold Certification
-- Checks: aggregate consistency, KPI thresholds, completeness
-- ============================================================
CREATE OR REPLACE PROCEDURE COCO_DE_DEMO.GOLD.validate_gold_gate()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  failures NUMBER DEFAULT 0;
  details VARCHAR DEFAULT '';
  fact_cnt NUMBER;
  order_cnt NUMBER;
  dim_cust_cnt NUMBER;
  stg_cust_cnt NUMBER;
  dim_prod_cnt NUMBER;
  stg_prod_cnt NUMBER;
  dim_dates_cnt NUMBER;
  ship_perf_cnt NUMBER;
  pay_summary_cnt NUMBER;
  daily_rev_cnt NUMBER;
  stg_ship_cnt NUMBER;
  null_fact_keys NUMBER;
  null_ship_keys NUMBER;
  null_pay_keys NUMBER;
  null_rev_keys NUMBER;
  invalid_tiers NUMBER;
  invalid_pay_outcomes NUMBER;
BEGIN
  SELECT COUNT(*) INTO :fact_cnt FROM COCO_DE_DEMO.GOLD.fact_sales;
  SELECT COUNT(*) INTO :order_cnt FROM COCO_DE_DEMO.SILVER.stg_orders;
  IF (:fact_cnt != :order_cnt) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: fact_sales rows (' || :fact_cnt
      || ') != stg_orders rows (' || :order_cnt || '). ';
  END IF;

  SELECT COUNT(*) INTO :dim_cust_cnt FROM COCO_DE_DEMO.GOLD.dim_customers;
  SELECT COUNT(*) INTO :stg_cust_cnt FROM COCO_DE_DEMO.SILVER.stg_customers;
  IF (:dim_cust_cnt != :stg_cust_cnt) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: dim_customers rows (' || :dim_cust_cnt
      || ') != stg_customers rows (' || :stg_cust_cnt || '). ';
  END IF;

  SELECT COUNT(*) INTO :dim_prod_cnt FROM COCO_DE_DEMO.GOLD.dim_products;
  SELECT COUNT(*) INTO :stg_prod_cnt FROM COCO_DE_DEMO.SILVER.stg_products;
  IF (:dim_prod_cnt != :stg_prod_cnt) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: dim_products rows (' || :dim_prod_cnt
      || ') != stg_products rows (' || :stg_prod_cnt || '). ';
  END IF;

  -- Validate new Gold tables are populated
  SELECT COUNT(*) INTO :dim_dates_cnt FROM COCO_DE_DEMO.GOLD.dim_dates;
  IF (:dim_dates_cnt = 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: dim_dates is empty. ';
  END IF;

  SELECT COUNT(*) INTO :ship_perf_cnt FROM COCO_DE_DEMO.GOLD.fact_shipment_performance;
  SELECT COUNT(*) INTO :stg_ship_cnt FROM COCO_DE_DEMO.SILVER.stg_shipments;
  IF (:ship_perf_cnt != :stg_ship_cnt) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: fact_shipment_performance rows (' || :ship_perf_cnt
      || ') != stg_shipments rows (' || :stg_ship_cnt || '). ';
  END IF;

  SELECT COUNT(*) INTO :pay_summary_cnt FROM COCO_DE_DEMO.GOLD.fact_payment_summary;
  IF (:pay_summary_cnt != :order_cnt) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: fact_payment_summary rows (' || :pay_summary_cnt
      || ') != stg_orders rows (' || :order_cnt || '). ';
  END IF;

  SELECT COUNT(*) INTO :daily_rev_cnt FROM COCO_DE_DEMO.GOLD.fact_daily_revenue;
  IF (:daily_rev_cnt = 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: fact_daily_revenue is empty. ';
  END IF;

  -- Null key checks across all Gold tables
  SELECT COUNT(*) INTO :null_fact_keys FROM COCO_DE_DEMO.GOLD.fact_sales WHERE order_id IS NULL;
  IF (:null_fact_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_fact_keys || ' null order_ids in fact_sales. ';
  END IF;

  SELECT COUNT(*) INTO :null_ship_keys FROM COCO_DE_DEMO.GOLD.fact_shipment_performance WHERE shipment_id IS NULL;
  IF (:null_ship_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_ship_keys || ' null shipment_ids in fact_shipment_performance. ';
  END IF;

  SELECT COUNT(*) INTO :null_pay_keys FROM COCO_DE_DEMO.GOLD.fact_payment_summary WHERE order_id IS NULL;
  IF (:null_pay_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_pay_keys || ' null order_ids in fact_payment_summary. ';
  END IF;

  SELECT COUNT(*) INTO :null_rev_keys FROM COCO_DE_DEMO.GOLD.fact_daily_revenue WHERE revenue_date IS NULL;
  IF (:null_rev_keys > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :null_rev_keys || ' null revenue_dates in fact_daily_revenue. ';
  END IF;

  -- Tier and classification validation
  SELECT COUNT(*) INTO :invalid_tiers FROM COCO_DE_DEMO.GOLD.dim_customers
  WHERE loyalty_tier NOT IN ('PLATINUM', 'GOLD', 'SILVER', 'BRONZE');
  IF (:invalid_tiers > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :invalid_tiers || ' invalid loyalty_tier values. ';
  END IF;

  SELECT COUNT(*) INTO :invalid_pay_outcomes FROM COCO_DE_DEMO.GOLD.fact_payment_summary
  WHERE payment_outcome NOT IN ('CLEAN', 'RECOVERED', 'ALL_FAILED', 'REFUNDED', 'NO_PAYMENT');
  IF (:invalid_pay_outcomes > 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: ' || :invalid_pay_outcomes || ' invalid payment_outcome values. ';
  END IF;

  IF (:fact_cnt = 0 OR :dim_cust_cnt = 0 OR :dim_prod_cnt = 0) THEN
    failures := :failures + 1;
    details := :details || 'FAIL: One or more core Gold tables are empty. ';
  END IF;

  IF (:failures > 0) THEN
    RETURN 'GATE 3 FAILED (' || :failures || ' checks): ' || :details;
  ELSE
    RETURN 'GATE 3 PASSED: Gold data certified for analytics consumption';
  END IF;
END;
$$;
