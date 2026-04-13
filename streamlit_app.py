import streamlit as st
import altair as alt
import pandas as pd
from datetime import timedelta
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="CoCo DE Pipeline Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main .block-container { padding-top: 1.5rem; }
    .metric-card {
        background: linear-gradient(135deg, #f8f9fc 0%, #ffffff 100%);
        border-radius: 10px;
        padding: 1.2rem 1rem 1rem 1rem;
        border-left: 4px solid #29B5E8;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
        margin-bottom: 0.5rem;
    }
    .metric-card .metric-label {
        font-size: 0.78rem;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.25rem;
    }
    .metric-card .metric-value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1e293b;
        line-height: 1.2;
    }
    .mc-revenue { border-left-color: #11b981; }
    .mc-orders  { border-left-color: #3b82f6; }
    .mc-aov     { border-left-color: #8b5cf6; }
    .mc-cust    { border-left-color: #f59e0b; }
    .mc-prod    { border-left-color: #ec4899; }
    .mc-units   { border-left-color: #06b6d4; }
    div[data-testid="stTabs"] button {
        font-size: 0.95rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div style="margin-bottom:0.8rem;">
    <span style="font-size:1.8rem; font-weight:800; color:#1e293b;">
        CoCo DE Pipeline Dashboard
    </span>
    <span style="font-size:0.9rem; color:#94a3b8; margin-left:0.6rem;">
        Gold Layer Analytics &middot; Medallion Architecture
    </span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Refresh button (clears cached data to show latest)
# ---------------------------------------------------------------------------
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# ---------------------------------------------------------------------------
# Session & data loading
# ---------------------------------------------------------------------------
session = get_active_session()

# Snowflake-blue palette for consistent charts
SF_PALETTE = ["#29B5E8", "#11b981", "#8b5cf6", "#f59e0b", "#ec4899", "#06b6d4", "#3b82f6"]


@st.cache_data(ttl=timedelta(minutes=5))
def load_daily_revenue():
    return session.sql("""
        SELECT
            ORDER_DATE::DATE AS REVENUE_DATE,
            COUNT(*) AS TOTAL_ORDERS,
            COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
            SUM(ORDER_TOTAL) AS GROSS_REVENUE,
            SUM(NET_REVENUE) AS NET_REVENUE,
            AVG(ORDER_TOTAL) AS AVG_ORDER_VALUE,
            SUM(CASE WHEN ORDER_STATUS = 'COMPLETED' THEN ORDER_TOTAL ELSE 0 END) AS COMPLETED_REVENUE,
            SUM(CASE WHEN ORDER_STATUS = 'CANCELLED' THEN ORDER_TOTAL ELSE 0 END) AS CANCELLED_REVENUE,
            SUM(CASE WHEN ORDER_STATUS = 'RETURNED' THEN ORDER_TOTAL ELSE 0 END) AS RETURNED_REVENUE,
            SUM(CASE WHEN CHANNEL = 'web' THEN 1 ELSE 0 END) AS WEB_ORDERS,
            SUM(CASE WHEN CHANNEL = 'mobile' THEN 1 ELSE 0 END) AS MOBILE_ORDERS,
            SUM(CASE WHEN CHANNEL = 'in-store' THEN 1 ELSE 0 END) AS INSTORE_ORDERS,
            SUM(CASE WHEN CHANNEL = 'phone' THEN 1 ELSE 0 END) AS PHONE_ORDERS,
            SUM(TOTAL_UNITS) AS TOTAL_UNITS_SOLD,
            SUM(TOTAL_DISCOUNT) AS TOTAL_DISCOUNT
        FROM COCO_DE_DEMO.GOLD.FACT_SALES
        GROUP BY ORDER_DATE::DATE
        ORDER BY REVENUE_DATE
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_segments():
    return session.sql("""
        SELECT SEGMENT, LOYALTY_TIER,
               COUNT(*) AS CUSTOMER_COUNT,
               SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
               SUM(LIFETIME_SPEND) AS TOTAL_SPEND,
               AVG(AVG_ORDER_VALUE) AS AVG_AOV,
               AVG(CANCELLATION_RATE) AS AVG_CANCEL_RATE
        FROM COCO_DE_DEMO.GOLD.DIM_CUSTOMERS
        GROUP BY SEGMENT, LOYALTY_TIER
        ORDER BY TOTAL_SPEND DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_top_products():
    return session.sql("""
        SELECT PRODUCT_NAME, CATEGORY, BRAND, PRICE_TIER, SALES_TIER,
               TOTAL_REVENUE, TOTAL_UNITS_SOLD, ORDERS_WITH_PRODUCT,
               MARGIN_PCT, PROFIT_MARGIN, STOCK_QUANTITY,
               IS_LOW_STOCK, DAYS_OF_STOCK_REMAINING
        FROM COCO_DE_DEMO.GOLD.DIM_PRODUCTS
        ORDER BY TOTAL_REVENUE DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_shipment_performance():
    return session.sql("""
        SELECT CARRIER, SHIPMENT_STATUS, SHIPPING_SPEED,
               COUNT(*) AS SHIPMENT_COUNT,
               AVG(DELIVERY_DAYS) AS AVG_DELIVERY_DAYS,
               AVG(DAYS_ORDER_TO_SHIP) AS AVG_DAYS_TO_SHIP,
               SUM(CASE WHEN IS_DELAYED THEN 1 ELSE 0 END) AS DELAYED_COUNT,
               SUM(CASE WHEN IS_DELIVERED THEN 1 ELSE 0 END) AS DELIVERED_COUNT
        FROM COCO_DE_DEMO.GOLD.FACT_SHIPMENT_PERFORMANCE
        GROUP BY CARRIER, SHIPMENT_STATUS, SHIPPING_SPEED
        ORDER BY SHIPMENT_COUNT DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_payment_summary():
    return session.sql("""
        SELECT PAYMENT_OUTCOME,
               COUNT(*) AS ORDER_COUNT,
               SUM(TOTAL_AMOUNT_COMPLETED) AS TOTAL_COMPLETED,
               SUM(TOTAL_AMOUNT_REFUNDED) AS TOTAL_REFUNDED,
               SUM(TOTAL_AMOUNT_FAILED) AS TOTAL_FAILED,
               AVG(PAYMENT_SUCCESS_RATE) AS AVG_SUCCESS_RATE
        FROM COCO_DE_DEMO.GOLD.FACT_PAYMENT_SUMMARY
        GROUP BY PAYMENT_OUTCOME
        ORDER BY ORDER_COUNT DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_rfm_segments():
    return session.sql("""
        SELECT RFM_SEGMENT, COUNT(*) AS CUSTOMER_COUNT,
               AVG(RECENCY_SCORE) AS AVG_R, AVG(FREQUENCY_SCORE) AS AVG_F,
               AVG(MONETARY_SCORE) AS AVG_M
        FROM COCO_DE_DEMO.GOLD.CUSTOMER_RFM_SCORES
        GROUP BY RFM_SEGMENT
        ORDER BY CUSTOMER_COUNT DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_anomaly_summary():
    return session.sql("""
        SELECT ANOMALY_TYPE, TABLE_NAME, COUNT(*) AS CNT
        FROM COCO_DE_DEMO.SILVER.ANOMALY_FLAGS
        GROUP BY ANOMALY_TYPE, TABLE_NAME
        ORDER BY CNT DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_profile_latest():
    return session.sql("""
        SELECT TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE
        FROM COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
        WHERE PROFILE_RUN_ID = (
            SELECT PROFILE_RUN_ID FROM COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
            ORDER BY PROFILED_AT DESC LIMIT 1
        )
        ORDER BY TABLE_NAME, COLUMN_NAME, METRIC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_pipeline_run():
    return session.sql("""
        SELECT LAYER, TABLE_NAME, ROW_COUNT, RUN_TIMESTAMP
        FROM COCO_DE_DEMO.GOLD.PIPELINE_RUN_SUMMARY
        WHERE RUN_ID = (
            SELECT RUN_ID FROM COCO_DE_DEMO.GOLD.PIPELINE_RUN_SUMMARY
            ORDER BY RUN_TIMESTAMP DESC LIMIT 1
        )
        ORDER BY LAYER, TABLE_NAME
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_kpi_totals():
    return session.sql("""
        SELECT
            (SELECT COUNT(*) FROM COCO_DE_DEMO.GOLD.DIM_CUSTOMERS) AS TOTAL_CUSTOMERS,
            (SELECT SUM(ORDER_TOTAL) FROM COCO_DE_DEMO.GOLD.FACT_SALES) AS TOTAL_REVENUE,
            (SELECT COUNT(*) FROM COCO_DE_DEMO.GOLD.FACT_SALES) AS TOTAL_ORDERS,
            (SELECT AVG(ORDER_TOTAL) FROM COCO_DE_DEMO.GOLD.FACT_SALES) AS AVG_ORDER_VALUE,
            (SELECT COUNT(*) FROM COCO_DE_DEMO.GOLD.DIM_PRODUCTS) AS TOTAL_PRODUCTS,
            (SELECT SUM(TOTAL_UNITS) FROM COCO_DE_DEMO.GOLD.FACT_SALES) AS TOTAL_UNITS_SOLD
    """).to_pandas()


# Load all data
daily_rev = load_daily_revenue()
kpis = load_kpi_totals().iloc[0]
customer_seg = load_customer_segments()
products = load_top_products()
shipments = load_shipment_performance()
payments = load_payment_summary()
rfm_seg = load_rfm_segments()
anomaly_summary = load_anomaly_summary()
profile_data = load_profile_latest()
pipeline_run = load_pipeline_run()

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------


def metric_card(label, value, css_class=""):
    st.markdown(f"""
    <div class="metric-card {css_class}">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
    </div>
    """, unsafe_allow_html=True)


k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    metric_card("Total Revenue", f"${kpis['TOTAL_REVENUE']:,.0f}", "mc-revenue")
with k2:
    metric_card("Total Orders", f"{kpis['TOTAL_ORDERS']:,.0f}", "mc-orders")
with k3:
    metric_card("Avg Order Value", f"${kpis['AVG_ORDER_VALUE']:,.2f}", "mc-aov")
with k4:
    metric_card("Customers", f"{kpis['TOTAL_CUSTOMERS']:,.0f}", "mc-cust")
with k5:
    metric_card("Products", f"{kpis['TOTAL_PRODUCTS']:,.0f}", "mc-prod")
with k6:
    metric_card("Units Sold", f"{kpis['TOTAL_UNITS_SOLD']:,.0f}", "mc-units")

st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_rev, tab_cust, tab_prod, tab_ops, tab_health = st.tabs([
    "Revenue & Sales",
    "Customers",
    "Products & Inventory",
    "Operations",
    "Pipeline Health",
])

# ========================== TAB 1: Revenue & Sales =========================
with tab_rev:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Daily Gross Revenue")
        chart = (
            alt.Chart(daily_rev)
            .mark_area(
                opacity=0.35,
                line={"color": "#29B5E8", "strokeWidth": 2},
                color=alt.Gradient(
                    gradient="linear",
                    stops=[
                        alt.GradientStop(color="#29B5E8", offset=0),
                        alt.GradientStop(color="white", offset=1),
                    ],
                    x1=1, x2=1, y1=0, y2=1,
                ),
            )
            .encode(
                x=alt.X("REVENUE_DATE:T", title="Date", axis=alt.Axis(grid=False)),
                y=alt.Y("GROSS_REVENUE:Q", title="Revenue ($)", axis=alt.Axis(format="$,.0f")),
                tooltip=[
                    alt.Tooltip("REVENUE_DATE:T", title="Date"),
                    alt.Tooltip("GROSS_REVENUE:Q", title="Revenue", format="$,.0f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("#### Revenue by Status (Monthly)")
        rev_breakdown = daily_rev[["REVENUE_DATE", "COMPLETED_REVENUE", "CANCELLED_REVENUE", "RETURNED_REVENUE"]].copy()
        rev_melted = rev_breakdown.melt(
            id_vars="REVENUE_DATE",
            value_vars=["COMPLETED_REVENUE", "CANCELLED_REVENUE", "RETURNED_REVENUE"],
            var_name="Type",
            value_name="Amount",
        )
        rev_melted["Type"] = rev_melted["Type"].str.replace("_REVENUE", "").str.title()
        chart = (
            alt.Chart(rev_melted)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("yearmonth(REVENUE_DATE):T", title="Month"),
                y=alt.Y("sum(Amount):Q", title="Revenue ($)", axis=alt.Axis(format="$,.0f")),
                color=alt.Color("Type:N", scale=alt.Scale(
                    domain=["Completed", "Cancelled", "Returned"],
                    range=["#11b981", "#ef4444", "#f59e0b"],
                ), legend=alt.Legend(orient="top", title=None)),
                tooltip=[
                    alt.Tooltip("yearmonth(REVENUE_DATE):T", title="Month"),
                    alt.Tooltip("Type:N"),
                    alt.Tooltip("sum(Amount):Q", title="Amount", format="$,.0f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("---")
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("#### Orders by Channel")
        channel_data = daily_rev[["WEB_ORDERS", "MOBILE_ORDERS", "INSTORE_ORDERS", "PHONE_ORDERS"]].sum().reset_index()
        channel_data.columns = ["Channel", "Orders"]
        channel_data["Channel"] = channel_data["Channel"].str.replace("_ORDERS", "").str.title()
        chart = (
            alt.Chart(channel_data)
            .mark_arc(innerRadius=60, outerRadius=120, cornerRadius=4)
            .encode(
                theta=alt.Theta("Orders:Q"),
                color=alt.Color("Channel:N", scale=alt.Scale(range=SF_PALETTE),
                                legend=alt.Legend(orient="right", title=None)),
                tooltip=[
                    alt.Tooltip("Channel:N"),
                    alt.Tooltip("Orders:Q", format=","),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)

    with col4:
        st.markdown("#### Daily Orders vs Customers")
        oc_data = daily_rev[["REVENUE_DATE", "TOTAL_ORDERS", "UNIQUE_CUSTOMERS"]].melt(
            id_vars="REVENUE_DATE",
            value_vars=["TOTAL_ORDERS", "UNIQUE_CUSTOMERS"],
            var_name="Metric",
            value_name="Count",
        )
        oc_data["Metric"] = oc_data["Metric"].map({
            "TOTAL_ORDERS": "Orders",
            "UNIQUE_CUSTOMERS": "Customers",
        })
        chart = (
            alt.Chart(oc_data)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("REVENUE_DATE:T", title="Date", axis=alt.Axis(grid=False)),
                y=alt.Y("Count:Q", title="Count"),
                color=alt.Color("Metric:N", scale=alt.Scale(
                    domain=["Orders", "Customers"],
                    range=["#3b82f6", "#f59e0b"],
                ), legend=alt.Legend(orient="top", title=None)),
                strokeDash=alt.StrokeDash("Metric:N", scale=alt.Scale(
                    domain=["Orders", "Customers"],
                    range=[[1, 0], [5, 3]],
                ), legend=None),
                tooltip=[
                    alt.Tooltip("REVENUE_DATE:T", title="Date"),
                    alt.Tooltip("Metric:N"),
                    alt.Tooltip("Count:Q", format=","),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)

# ========================== TAB 2: Customers ===============================
with tab_cust:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Spend by Segment")
        seg_summary = customer_seg.groupby("SEGMENT", as_index=False).agg(
            TOTAL_SPEND=("TOTAL_SPEND", "sum"),
            CUSTOMER_COUNT=("CUSTOMER_COUNT", "sum"),
        )
        chart = (
            alt.Chart(seg_summary)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("SEGMENT:N", title="Segment", sort="-y", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("TOTAL_SPEND:Q", title="Total Spend ($)", axis=alt.Axis(format="$,.0f")),
                color=alt.Color("SEGMENT:N", legend=alt.Legend(orient="top", title="Segment"), scale=alt.Scale(range=SF_PALETTE)),
                tooltip=[
                    alt.Tooltip("SEGMENT:N"),
                    alt.Tooltip("TOTAL_SPEND:Q", format="$,.0f"),
                    alt.Tooltip("CUSTOMER_COUNT:Q", title="Customers", format=","),
                ],
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("#### Loyalty Tier Distribution")
        loyalty_summary = customer_seg.groupby("LOYALTY_TIER", as_index=False).agg(
            CUSTOMER_COUNT=("CUSTOMER_COUNT", "sum")
        )
        chart = (
            alt.Chart(loyalty_summary)
            .mark_arc(innerRadius=60, outerRadius=120, cornerRadius=4)
            .encode(
                theta=alt.Theta("CUSTOMER_COUNT:Q"),
                color=alt.Color("LOYALTY_TIER:N", scale=alt.Scale(
                    domain=["Platinum", "Gold", "Silver", "Bronze"],
                    range=["#06b6d4", "#f59e0b", "#94a3b8", "#d97706"],
                ), legend=alt.Legend(orient="right", title=None)),
                tooltip=[
                    alt.Tooltip("LOYALTY_TIER:N", title="Tier"),
                    alt.Tooltip("CUSTOMER_COUNT:Q", title="Customers", format=","),
                ],
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Segment Detail")
    seg_detail = customer_seg.groupby("SEGMENT", as_index=False).agg(
        CUSTOMER_COUNT=("CUSTOMER_COUNT", "sum"),
        TOTAL_ORDERS=("TOTAL_ORDERS", "sum"),
        TOTAL_SPEND=("TOTAL_SPEND", "sum"),
        AVG_AOV=("AVG_AOV", "mean"),
        AVG_CANCEL_RATE=("AVG_CANCEL_RATE", "mean"),
    )
    seg_detail = seg_detail.sort_values("TOTAL_SPEND", ascending=False)
    seg_display = seg_detail.rename(columns={
        "SEGMENT": "Segment",
        "CUSTOMER_COUNT": "Customers",
        "TOTAL_ORDERS": "Orders",
        "TOTAL_SPEND": "Lifetime Spend",
        "AVG_AOV": "Avg Order Value",
        "AVG_CANCEL_RATE": "Cancel Rate",
    })
    seg_display["Lifetime Spend"] = seg_display["Lifetime Spend"].apply(lambda x: f"${x:,.0f}")
    seg_display["Avg Order Value"] = seg_display["Avg Order Value"].apply(lambda x: f"${x:,.2f}")
    seg_display["Cancel Rate"] = seg_display["Cancel Rate"].apply(lambda x: f"{x:.1f}%")
    seg_display["Customers"] = seg_display["Customers"].astype(int)
    seg_display["Orders"] = seg_display["Orders"].astype(int)
    st.dataframe(seg_display, use_container_width=True)

# ========================== TAB 3: Products & Inventory ====================
with tab_prod:
    st.markdown("#### Top 15 Products by Revenue")
    top_n = products.head(15).copy()
    top_display = top_n.drop(columns=["PROFIT_MARGIN"], errors="ignore")
    top_display = top_display.rename(columns={
        "PRODUCT_NAME": "Product",
        "CATEGORY": "Category",
        "BRAND": "Brand",
        "PRICE_TIER": "Price Tier",
        "SALES_TIER": "Sales Tier",
        "TOTAL_REVENUE": "Revenue",
        "TOTAL_UNITS_SOLD": "Units Sold",
        "ORDERS_WITH_PRODUCT": "Orders",
        "MARGIN_PCT": "Margin %",
        "STOCK_QUANTITY": "Stock",
        "IS_LOW_STOCK": "Low Stock",
        "DAYS_OF_STOCK_REMAINING": "Days of Stock",
    })
    top_display["Revenue"] = top_display["Revenue"].apply(lambda x: f"${x:,.0f}")
    top_display["Margin %"] = top_display["Margin %"].apply(lambda x: f"{x:.0f}%")
    top_display["Days of Stock"] = top_display["Days of Stock"].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "")
    st.dataframe(top_display, use_container_width=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Revenue by Category")
        cat_data = products.groupby("CATEGORY", as_index=False).agg(
            TOTAL_REVENUE=("TOTAL_REVENUE", "sum")
        ).sort_values("TOTAL_REVENUE", ascending=False)
        chart = (
            alt.Chart(cat_data)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("TOTAL_REVENUE:Q", title="Revenue ($)", axis=alt.Axis(format="$,.0f")),
                y=alt.Y("CATEGORY:N", title="Category", sort="-x"),
                color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="top", title="Category"), scale=alt.Scale(range=SF_PALETTE)),
                tooltip=[
                    alt.Tooltip("CATEGORY:N"),
                    alt.Tooltip("TOTAL_REVENUE:Q", format="$,.0f"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("#### Low Stock Alerts")
        low_stock = products[products["IS_LOW_STOCK"] == True][
            ["PRODUCT_NAME", "CATEGORY", "STOCK_QUANTITY", "TOTAL_UNITS_SOLD", "DAYS_OF_STOCK_REMAINING"]
        ].sort_values("STOCK_QUANTITY")
        if len(low_stock) > 0:
            st.warning(f"{len(low_stock)} product(s) with low stock levels")
            low_display = low_stock.rename(columns={
                "PRODUCT_NAME": "Product",
                "CATEGORY": "Category",
                "STOCK_QUANTITY": "Stock",
                "TOTAL_UNITS_SOLD": "Units Sold",
                "DAYS_OF_STOCK_REMAINING": "Days Left",
            })
            low_display["Days Left"] = low_display["Days Left"].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "")
            st.dataframe(low_display, use_container_width=True)
        else:
            st.success("All products are adequately stocked.")

# ========================== TAB 4: Operations ==============================
with tab_ops:
    # --- Shipment section ---
    st.markdown("#### Shipment Performance")
    col1, col2 = st.columns(2)

    with col1:
        carrier_data = shipments.groupby("CARRIER", as_index=False).agg(
            SHIPMENT_COUNT=("SHIPMENT_COUNT", "sum"),
            DELIVERED_COUNT=("DELIVERED_COUNT", "sum"),
            DELAYED_COUNT=("DELAYED_COUNT", "sum"),
            AVG_DELIVERY_DAYS=("AVG_DELIVERY_DAYS", "mean"),
        )
        carrier_data["ON_TIME_PCT"] = (
            (carrier_data["DELIVERED_COUNT"] - carrier_data["DELAYED_COUNT"])
            / carrier_data["DELIVERED_COUNT"].replace(0, 1)
            * 100
        )
        chart = (
            alt.Chart(carrier_data)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("CARRIER:N", title="Carrier", sort="-y", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("SHIPMENT_COUNT:Q", title="Shipments"),
                color=alt.Color("CARRIER:N", legend=alt.Legend(orient="top", title="Carrier"), scale=alt.Scale(range=SF_PALETTE)),
                tooltip=[
                    alt.Tooltip("CARRIER:N"),
                    alt.Tooltip("SHIPMENT_COUNT:Q", title="Total", format=","),
                    alt.Tooltip("DELAYED_COUNT:Q", title="Delayed", format=","),
                    alt.Tooltip("AVG_DELIVERY_DAYS:Q", title="Avg Days", format=".1f"),
                    alt.Tooltip("ON_TIME_PCT:Q", title="On-Time %", format=".1f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        speed_data = shipments.groupby("SHIPPING_SPEED", as_index=False).agg(
            SHIPMENT_COUNT=("SHIPMENT_COUNT", "sum")
        )
        chart = (
            alt.Chart(speed_data)
            .mark_arc(innerRadius=60, outerRadius=120, cornerRadius=4)
            .encode(
                theta=alt.Theta("SHIPMENT_COUNT:Q"),
                color=alt.Color("SHIPPING_SPEED:N", scale=alt.Scale(
                    domain=["Express", "Standard", "Slow"],
                    range=["#11b981", "#3b82f6", "#ef4444"],
                ), legend=alt.Legend(orient="right", title=None)),
                tooltip=[
                    alt.Tooltip("SHIPPING_SPEED:N", title="Speed"),
                    alt.Tooltip("SHIPMENT_COUNT:Q", title="Count", format=","),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

    # --- Payment section ---
    st.markdown("---")
    st.markdown("#### Payment Health")

    total_completed = float(payments["TOTAL_COMPLETED"].sum())
    total_refunded = float(payments["TOTAL_REFUNDED"].sum())
    total_failed = float(payments["TOTAL_FAILED"].sum())
    avg_success = float(payments["AVG_SUCCESS_RATE"].mean()) * 100

    p1, p2, p3, p4 = st.columns(4)
    with p1:
        metric_card("Completed", f"${total_completed:,.0f}", "mc-revenue")
    with p2:
        metric_card("Refunded", f"${total_refunded:,.0f}", "mc-aov")
    with p3:
        metric_card("Failed", f"${total_failed:,.0f}", "mc-prod")
    with p4:
        metric_card("Success Rate", f"{avg_success:.1f}%", "mc-orders")

    chart = (
        alt.Chart(payments)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("PAYMENT_OUTCOME:N", title="Payment Outcome", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("ORDER_COUNT:Q", title="Orders"),
            color=alt.Color("PAYMENT_OUTCOME:N", scale=alt.Scale(
                domain=["Fully Paid", "Partial", "Unpaid", "Refunded", "Failed"],
                range=["#11b981", "#f59e0b", "#ef4444", "#8b5cf6", "#94a3b8"],
            ), legend=alt.Legend(orient="top", title="Outcome")),
            tooltip=[
                alt.Tooltip("PAYMENT_OUTCOME:N"),
                alt.Tooltip("ORDER_COUNT:Q", format=","),
                alt.Tooltip("AVG_SUCCESS_RATE:Q", title="Success Rate", format=".1%"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

# ========================== TAB 5: Pipeline Health ==========================
with tab_health:
    st.markdown("#### Snowpark Python Processing Results")
    st.markdown("_Hybrid SQL + Python pipeline: dbt for transformations, Snowpark for profiling, anomaly detection & enrichment._")

    # --- RFM Segment Distribution ---
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Customer RFM Segments")
        if len(rfm_seg) > 0:
            chart = (
                alt.Chart(rfm_seg)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("RFM_SEGMENT:N", title="Segment", sort="-y", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("CUSTOMER_COUNT:Q", title="Customers"),
                    color=alt.Color("RFM_SEGMENT:N", legend=alt.Legend(orient="top", title="RFM Segment"), scale=alt.Scale(
                        domain=["CHAMPION", "LOYAL", "AT_RISK", "HIBERNATING"],
                        range=["#11b981", "#3b82f6", "#f59e0b", "#ef4444"],
                    )),
                    tooltip=[
                        alt.Tooltip("RFM_SEGMENT:N", title="Segment"),
                        alt.Tooltip("CUSTOMER_COUNT:Q", title="Customers", format=","),
                        alt.Tooltip("AVG_R:Q", title="Avg Recency Score", format=".1f"),
                        alt.Tooltip("AVG_F:Q", title="Avg Frequency Score", format=".1f"),
                        alt.Tooltip("AVG_M:Q", title="Avg Monetary Score", format=".1f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No RFM scores computed yet. Run the Gold Enrichment procedure.")

    with col2:
        st.markdown("#### Anomaly Detection Summary")
        if len(anomaly_summary) > 0:
            total_anomalies = int(anomaly_summary["CNT"].sum())
            a1, a2 = st.columns(2)
            with a1:
                metric_card("Total Anomalies", f"{total_anomalies:,}", "mc-prod")
            with a2:
                metric_card("Anomaly Types", f"{len(anomaly_summary)}", "mc-aov")

            anom_display = anomaly_summary.rename(columns={
                "ANOMALY_TYPE": "Type",
                "TABLE_NAME": "Source Table",
                "CNT": "Count",
            })
            st.dataframe(anom_display, use_container_width=True)
        else:
            st.info("No anomalies detected yet. Run the Anomaly Detector procedure.")

    # --- Pipeline Run Summary ---
    st.markdown("---")
    st.markdown("#### Pipeline Row Counts (Latest Run)")
    if len(pipeline_run) > 0:
        run_ts = pipeline_run["RUN_TIMESTAMP"].iloc[0]
        st.caption(f"Last run: {run_ts}")

        chart = (
            alt.Chart(pipeline_run)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("TABLE_NAME:N", title="Table", sort="-y", axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("ROW_COUNT:Q", title="Row Count"),
                color=alt.Color("LAYER:N", scale=alt.Scale(
                    domain=["BRONZE", "SILVER", "GOLD"],
                    range=["#d97706", "#94a3b8", "#f59e0b"],
                ), legend=alt.Legend(orient="top", title="Layer")),
                tooltip=[
                    alt.Tooltip("LAYER:N"),
                    alt.Tooltip("TABLE_NAME:N"),
                    alt.Tooltip("ROW_COUNT:Q", title="Rows", format=","),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No pipeline run summary yet. Run the Gold Enrichment procedure.")

    # --- Bronze Data Profile ---
    st.markdown("---")
    st.markdown("#### Bronze Data Profile (Latest)")
    if len(profile_data) > 0:
        row_counts = profile_data[profile_data["METRIC"] == "ROW_COUNT"][["TABLE_NAME", "METRIC_VALUE"]].copy()
        row_counts = row_counts.rename(columns={"TABLE_NAME": "Table", "METRIC_VALUE": "Row Count"})
        row_counts["Row Count"] = row_counts["Row Count"].apply(lambda x: f"{int(x):,}")
        st.dataframe(row_counts, use_container_width=True)

        null_data = profile_data[profile_data["METRIC"] == "NULL_PCT"].copy()
        null_data["METRIC_VALUE"] = null_data["METRIC_VALUE"].astype(float)
        high_nulls = null_data[null_data["METRIC_VALUE"] > 0].sort_values("METRIC_VALUE", ascending=False).head(15)
        if len(high_nulls) > 0:
            st.markdown("**Columns with Nulls:**")
            null_display = high_nulls[["TABLE_NAME", "COLUMN_NAME", "METRIC_VALUE"]].rename(columns={
                "TABLE_NAME": "Table",
                "COLUMN_NAME": "Column",
                "METRIC_VALUE": "Null %",
            })
            null_display["Null %"] = null_display["Null %"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(null_display, use_container_width=True)
        else:
            st.success("No null values detected across Bronze tables.")
    else:
        st.info("No profile data yet. Run the Bronze Profiler procedure.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption("COCO_DE_DEMO Gold Layer  |  Auto-refreshes every 5 minutes  |  Hybrid SQL + Python Medallion Pipeline")
