import streamlit as st
import altair as alt
import pandas as pd
from datetime import timedelta
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="CoCo DE Governance Dashboard",
    page_icon=":shield:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Custom CSS  (matches pipeline dashboard styling)
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
    .mc-tags    { border-left-color: #11b981; }
    .mc-pii     { border-left-color: #ef4444; }
    .mc-dmf     { border-left-color: #3b82f6; }
    .mc-anomaly { border-left-color: #f59e0b; }
    .mc-tables  { border-left-color: #8b5cf6; }
    .mc-pass    { border-left-color: #11b981; }
    .mc-fail    { border-left-color: #ef4444; }
    .mc-fresh   { border-left-color: #06b6d4; }
    div[data-testid="stTabs"] button {
        font-size: 0.95rem;
        font-weight: 600;
    }
    .status-pass { color: #11b981; font-weight: 700; }
    .status-fail { color: #ef4444; font-weight: 700; }
    .status-warn { color: #f59e0b; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div style="margin-bottom:0.8rem;">
    <span style="font-size:1.8rem; font-weight:800; color:#1e293b;">
        CoCo DE Governance Dashboard
    </span>
    <span style="font-size:0.9rem; color:#94a3b8; margin-left:0.6rem;">
        Data Quality &middot; PII Tracking &middot; Tag Coverage &middot; Anomaly Detection
    </span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# ---------------------------------------------------------------------------
# Session & palette
# ---------------------------------------------------------------------------
session = get_active_session()
SF_PALETTE = ["#29B5E8", "#11b981", "#8b5cf6", "#f59e0b", "#ef4444", "#06b6d4", "#3b82f6"]


def metric_card(label, value, css_class=""):
    cls = f"metric-card {css_class}" if css_class else "metric-card"
    st.markdown(
        f'<div class="{cls}">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=timedelta(minutes=5))
def load_tag_references():
    return session.sql("""
        SELECT TAG_NAME, TAG_VALUE, OBJECT_DATABASE, OBJECT_SCHEMA,
               OBJECT_NAME, DOMAIN, COLUMN_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE OBJECT_DATABASE = 'COCO_DE_DEMO'
          AND TAG_DATABASE = 'COCO_DE_DEMO'
          AND TAG_SCHEMA = 'GOVERNANCE'
        ORDER BY OBJECT_SCHEMA, OBJECT_NAME, TAG_NAME
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_dmf_results():
    return session.sql("""
        SELECT MEASUREMENT_TIME, TABLE_NAME, TABLE_SCHEMA,
               METRIC_NAME, METRIC_SCHEMA, METRIC_DATABASE,
               ARGUMENT_NAMES, VALUE
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
        WHERE TABLE_DATABASE = 'COCO_DE_DEMO'
        ORDER BY MEASUREMENT_TIME DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_dmf_references():
    tables = [
        'COCO_DE_DEMO.BRONZE.CUSTOMERS', 'COCO_DE_DEMO.BRONZE.ORDERS',
        'COCO_DE_DEMO.BRONZE.PRODUCTS', 'COCO_DE_DEMO.BRONZE.ORDER_ITEMS',
        'COCO_DE_DEMO.BRONZE.PAYMENTS', 'COCO_DE_DEMO.BRONZE.SHIPMENTS',
        'COCO_DE_DEMO.SILVER.STG_CUSTOMERS', 'COCO_DE_DEMO.SILVER.STG_ORDERS',
        'COCO_DE_DEMO.SILVER.STG_PRODUCTS',
        'COCO_DE_DEMO.GOLD.DIM_CUSTOMERS', 'COCO_DE_DEMO.GOLD.DIM_PRODUCTS',
        'COCO_DE_DEMO.GOLD.FACT_SALES',
    ]
    frames = []
    for t in tables:
        try:
            df = session.sql(f"""
                SELECT METRIC_NAME, REF_ENTITY_NAME, REF_ARGUMENTS,
                       SCHEDULE, SCHEDULE_STATUS
                FROM TABLE(COCO_DE_DEMO.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
                    REF_ENTITY_NAME => '{t}',
                    REF_ENTITY_DOMAIN => 'TABLE'
                ))
            """).to_pandas()
            schema = t.split('.')[1]
            df['TABLE_SCHEMA'] = schema
            frames.append(df)
        except Exception:
            pass
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


@st.cache_data(ttl=timedelta(minutes=5))
def load_anomaly_flags():
    return session.sql("""
        SELECT RECORD_ID, ANOMALY_TYPE, DETAILS, DETECTED_AT
        FROM COCO_DE_DEMO.SILVER.ANOMALY_FLAGS
        ORDER BY DETECTED_AT DESC
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_table_inventory():
    return session.sql("""
        SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES,
               LAST_ALTERED
        FROM COCO_DE_DEMO.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('BRONZE','SILVER','GOLD')
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def load_profile_log():
    return session.sql("""
        SELECT TABLE_NAME, COLUMN_NAME, METRIC, METRIC_VALUE, PROFILED_AT
        FROM COCO_DE_DEMO.BRONZE.DATA_PROFILE_LOG
        ORDER BY PROFILED_AT DESC
    """).to_pandas()


# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
tags_df = load_tag_references()
dmf_results_df = load_dmf_results()
dmf_refs_df = load_dmf_references()
anomaly_df = load_anomaly_flags()
tables_df = load_table_inventory()
profile_df = load_profile_log()

# ═══════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Governance Overview",
    "Data Quality (DMFs)",
    "PII & Classification",
    "Anomaly Detection",
    "Pipeline Lineage & Health",
])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — GOVERNANCE OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Governance Overview")

    # KPI row
    table_tags = tags_df[tags_df['DOMAIN'] == 'TABLE']
    col_tags = tags_df[tags_df['DOMAIN'] == 'COLUMN']
    pii_cols = col_tags[
        (col_tags['TAG_NAME'] == 'PII') & (col_tags['TAG_VALUE'] == 'TRUE')
    ]
    total_tables = len(tables_df)
    tagged_tables = table_tags['OBJECT_NAME'].nunique()
    total_dmfs = len(dmf_refs_df) if not dmf_refs_df.empty else 0
    total_anomalies = len(anomaly_df)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        metric_card("Tagged Tables", f"{tagged_tables} / {total_tables}", "mc-tags")
    with c2:
        metric_card("PII Columns Tracked", str(len(pii_cols)), "mc-pii")
    with c3:
        metric_card("Active DMFs", str(total_dmfs), "mc-dmf")
    with c4:
        metric_card("Anomalies Detected", f"{total_anomalies:,}", "mc-anomaly")
    with c5:
        dmf_started = len(dmf_refs_df[dmf_refs_df['SCHEDULE_STATUS'] == 'STARTED']) if not dmf_refs_df.empty else 0
        metric_card("DMFs Running", str(dmf_started), "mc-pass")

    st.markdown("---")

    # Tag coverage heatmap — table vs tag type
    st.markdown("#### Tag Coverage Matrix")
    if not table_tags.empty:
        all_tables_list = tables_df[['TABLE_SCHEMA', 'TABLE_NAME']].copy()
        all_tables_list['FULL_NAME'] = all_tables_list['TABLE_SCHEMA'] + '.' + all_tables_list['TABLE_NAME']

        tag_types = ['PIPELINE_LAYER', 'DATA_CLASSIFICATION', 'DATA_DOMAIN', 'QUALITY_TIER']
        heatmap_rows = []
        for _, row in all_tables_list.iterrows():
            for tag in tag_types:
                match = table_tags[
                    (table_tags['OBJECT_SCHEMA'] == row['TABLE_SCHEMA']) &
                    (table_tags['OBJECT_NAME'] == row['TABLE_NAME']) &
                    (table_tags['TAG_NAME'] == tag)
                ]
                val = match['TAG_VALUE'].iloc[0] if len(match) > 0 else 'MISSING'
                heatmap_rows.append({
                    'Table': row['FULL_NAME'],
                    'Tag': tag,
                    'Value': val,
                    'Present': 1 if val != 'MISSING' else 0,
                })
        hm_df = pd.DataFrame(heatmap_rows)

        chart = alt.Chart(hm_df).mark_rect(stroke='white', strokeWidth=2).encode(
            x=alt.X('Tag:N', title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y('Table:N', title=None, sort=alt.SortField('Table')),
            color=alt.condition(
                alt.datum.Present == 1,
                alt.value('#11b981'),
                alt.value('#fee2e2'),
            ),
            tooltip=['Table', 'Tag', 'Value'],
        ).properties(height=max(len(all_tables_list) * 28, 200))

        text = alt.Chart(hm_df).mark_text(fontSize=10, color='#1e293b').encode(
            x='Tag:N',
            y=alt.Y('Table:N', sort=alt.SortField('Table')),
            text='Value:N',
        )

        st.altair_chart(chart + text, use_container_width=True)
    else:
        st.info("No table-level tags found.")

    # Tag type distribution
    st.markdown("#### Tag Distribution by Type")
    if not tags_df.empty:
        tag_dist = tags_df.groupby('TAG_NAME').size().reset_index(name='COUNT')
        bar = alt.Chart(tag_dist).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X('TAG_NAME:N', title='Tag Type', sort='-y'),
            y=alt.Y('COUNT:Q', title='Assignments'),
            color=alt.Color('TAG_NAME:N', scale=alt.Scale(range=SF_PALETTE), legend=None),
            tooltip=['TAG_NAME', 'COUNT'],
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — DATA QUALITY (DMFs)
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Data Quality Monitoring (DMFs)")

    # DMF inventory
    st.markdown("#### DMF Inventory")
    if not dmf_refs_df.empty:
        inv = dmf_refs_df[['TABLE_SCHEMA', 'REF_ENTITY_NAME', 'METRIC_NAME',
                           'REF_ARGUMENTS', 'SCHEDULE', 'SCHEDULE_STATUS']].copy()
        inv.columns = ['Schema', 'Table', 'DMF', 'Arguments', 'Schedule', 'Status']
        st.dataframe(inv, use_container_width=True)
    else:
        st.info("No DMF references found.")

    st.markdown("---")

    # Latest DMF results per metric+table
    st.markdown("#### Latest DMF Results")
    if not dmf_results_df.empty:
        dmf_results_df['MEASUREMENT_TIME'] = pd.to_datetime(
            dmf_results_df['MEASUREMENT_TIME'].astype(str).str.strip('"')
        )
        dmf_results_df['VALUE'] = pd.to_numeric(dmf_results_df['VALUE'], errors='coerce')

        latest_idx = dmf_results_df.groupby(
            ['TABLE_SCHEMA', 'TABLE_NAME', 'METRIC_NAME']
        )['MEASUREMENT_TIME'].idxmax()
        latest = dmf_results_df.loc[latest_idx].copy()

        # For null/dup checks, 0 = pass; for row_count, value > 0 = pass
        def quality_status(row):
            name = row['METRIC_NAME']
            val = row['VALUE']
            if name in ('NULL_COUNT', 'DUPLICATE_COUNT'):
                return 'PASS' if val == 0 else f'FAIL ({int(val)})'
            if name == 'ROW_COUNT':
                return 'OK' if val > 0 else 'EMPTY'
            if name == 'FRESHNESS':
                return f'{int(val)}s ago'
            if name in ('DMF_POSITIVE_AMOUNTS', 'DMF_ORDERS_CUSTOMER_FK'):
                return 'PASS' if val == 0 else f'FAIL ({int(val)})'
            return str(int(val))

        latest['STATUS'] = latest.apply(quality_status, axis=1)
        display_cols = ['TABLE_SCHEMA', 'TABLE_NAME', 'METRIC_NAME',
                        'ARGUMENT_NAMES', 'VALUE', 'STATUS', 'MEASUREMENT_TIME']
        show_df = latest[display_cols].sort_values(
            ['TABLE_SCHEMA', 'TABLE_NAME', 'METRIC_NAME']
        ).copy()
        show_df.columns = ['Schema', 'Table', 'DMF', 'Column(s)', 'Value',
                           'Status', 'Measured At']
        st.dataframe(show_df, use_container_width=True)

        # KPI: pass vs fail
        pass_count = latest['STATUS'].str.startswith('PASS').sum() + \
                     latest['STATUS'].str.startswith('OK').sum()
        fail_count = latest['STATUS'].str.contains('FAIL').sum()
        c1, c2, c3 = st.columns(3)
        with c1:
            metric_card("Checks Passing", str(int(pass_count)), "mc-pass")
        with c2:
            metric_card("Checks Failing", str(int(fail_count)), "mc-fail")
        with c3:
            metric_card("Total Results", f"{len(dmf_results_df):,}", "mc-dmf")

        st.markdown("---")

        # DMF trend over time
        st.markdown("#### DMF Value Trends")
        metric_filter = st.selectbox(
            "Select DMF to chart",
            sorted(dmf_results_df['METRIC_NAME'].unique()),
            key="dmf_trend_select",
        )
        trend_data = dmf_results_df[dmf_results_df['METRIC_NAME'] == metric_filter].copy()
        if not trend_data.empty:
            trend_data['LABEL'] = trend_data['TABLE_SCHEMA'] + '.' + trend_data['TABLE_NAME']
            line = alt.Chart(trend_data).mark_line(point=True).encode(
                x=alt.X('MEASUREMENT_TIME:T', title='Time'),
                y=alt.Y('VALUE:Q', title='Value'),
                color=alt.Color('LABEL:N', scale=alt.Scale(range=SF_PALETTE), title='Table'),
                tooltip=['LABEL', 'VALUE', 'MEASUREMENT_TIME:T'],
            ).properties(height=350)
            st.altair_chart(line, use_container_width=True)
    else:
        st.info("No DMF results available yet. DMFs run on an hourly schedule.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 — PII & CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("PII & Data Classification")

    col_tags = tags_df[tags_df['DOMAIN'] == 'COLUMN']
    table_tags_only = tags_df[tags_df['DOMAIN'] == 'TABLE']

    # PII inventory
    st.markdown("#### PII Column Inventory")
    pii_all = col_tags[
        (col_tags['TAG_NAME'] == 'PII') & (col_tags['TAG_VALUE'] == 'TRUE')
    ][['OBJECT_SCHEMA', 'OBJECT_NAME', 'COLUMN_NAME']].copy()
    pii_all.columns = ['Schema', 'Table', 'Column']

    if not pii_all.empty:
        c1, c2, c3 = st.columns(3)
        bronze_pii = pii_all[pii_all['Schema'] == 'BRONZE']
        silver_pii = pii_all[pii_all['Schema'] == 'SILVER']
        gold_pii = pii_all[pii_all['Schema'] == 'GOLD']
        with c1:
            metric_card("Bronze PII Columns", str(len(bronze_pii)), "mc-pii")
        with c2:
            metric_card("Silver PII Columns", str(len(silver_pii)), "mc-pii")
        with c3:
            metric_card("Gold PII Columns", str(len(gold_pii)), "mc-pii")

        st.dataframe(pii_all.sort_values(['Schema', 'Table', 'Column']),
                      use_container_width=True)
    else:
        st.warning("No PII columns tagged.")

    st.markdown("---")

    # PII propagation tracker
    st.markdown("#### PII Propagation Across Layers")
    st.caption("Tracks whether PII columns tagged in Bronze are also tagged in Silver and Gold.")
    if not pii_all.empty:
        bronze_cols = set(bronze_pii['Column'].tolist()) if not bronze_pii.empty else set()
        silver_cols = set(silver_pii['Column'].tolist()) if not silver_pii.empty else set()
        gold_cols = set(gold_pii['Column'].tolist()) if not gold_pii.empty else set()
        all_pii_cols = bronze_cols | silver_cols | gold_cols

        prop_rows = []
        for col in sorted(all_pii_cols):
            prop_rows.append({
                'PII Column': col,
                'Bronze': 'Tagged' if col in bronze_cols else '-',
                'Silver': 'Tagged' if col in silver_cols else '-',
                'Gold': 'Tagged' if col in gold_cols else '-',
            })
        prop_df = pd.DataFrame(prop_rows)
        st.dataframe(prop_df, use_container_width=True)

    st.markdown("---")

    # Data classification distribution
    st.markdown("#### Data Classification Distribution")
    class_tags = table_tags_only[table_tags_only['TAG_NAME'] == 'DATA_CLASSIFICATION']
    if not class_tags.empty:
        class_dist = class_tags.groupby('TAG_VALUE').size().reset_index(name='COUNT')
        pie = alt.Chart(class_dist).mark_arc(innerRadius=50).encode(
            theta=alt.Theta('COUNT:Q'),
            color=alt.Color('TAG_VALUE:N', scale=alt.Scale(
                domain=['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'],
                range=['#11b981', '#3b82f6', '#f59e0b', '#ef4444'],
            ), title='Classification'),
            tooltip=['TAG_VALUE', 'COUNT'],
        ).properties(height=300)
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("No DATA_CLASSIFICATION tags found.")

    # Quality tier breakdown
    st.markdown("#### Quality Tier by Layer")
    qt_tags = table_tags_only[table_tags_only['TAG_NAME'] == 'QUALITY_TIER']
    if not qt_tags.empty:
        qt_grouped = qt_tags.groupby(['OBJECT_SCHEMA', 'TAG_VALUE']).size().reset_index(name='COUNT')
        qt_grouped.columns = ['Schema', 'Quality Tier', 'Count']
        bar = alt.Chart(qt_grouped).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X('Schema:N', title='Layer'),
            y=alt.Y('Count:Q', title='Tables'),
            color=alt.Color('Quality Tier:N', scale=alt.Scale(
                domain=['RAW', 'VALIDATED', 'CERTIFIED'],
                range=['#f59e0b', '#3b82f6', '#11b981'],
            )),
            tooltip=['Schema', 'Quality Tier', 'Count'],
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 — ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Anomaly Detection")

    if not anomaly_df.empty:
        anomaly_df['DETECTED_AT'] = pd.to_datetime(
            anomaly_df['DETECTED_AT'].astype(str).str.strip('"')
        )
        total = len(anomaly_df)
        by_type = anomaly_df['ANOMALY_TYPE'].value_counts()

        c1, c2, c3 = st.columns(3)
        with c1:
            metric_card("Total Anomalies", f"{total:,}", "mc-anomaly")
        with c2:
            dormant = int(by_type.get('DORMANT_CUSTOMER', 0))
            metric_card("Dormant Customers", f"{dormant:,}", "mc-fail")
        with c3:
            outliers = int(by_type.get('AMOUNT_OUTLIER', 0))
            metric_card("Amount Outliers", f"{outliers:,}", "mc-pii")

        st.markdown("---")

        # Anomaly type breakdown
        st.markdown("#### Anomalies by Type")
        type_df = by_type.reset_index()
        type_df.columns = ['Anomaly Type', 'Count']
        bar = alt.Chart(type_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X('Anomaly Type:N', title=None),
            y=alt.Y('Count:Q', title='Count'),
            color=alt.Color('Anomaly Type:N', scale=alt.Scale(range=SF_PALETTE), legend=None),
            tooltip=['Anomaly Type', 'Count'],
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)

        # Trend over time
        st.markdown("#### Anomaly Detection Timeline")
        anomaly_df['DATE'] = anomaly_df['DETECTED_AT'].dt.date
        trend = anomaly_df.groupby(['DATE', 'ANOMALY_TYPE']).size().reset_index(name='COUNT')
        trend['DATE'] = pd.to_datetime(trend['DATE'])
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('DATE:T', title='Date'),
            y=alt.Y('COUNT:Q', title='Anomalies'),
            color=alt.Color('ANOMALY_TYPE:N', scale=alt.Scale(range=SF_PALETTE), title='Type'),
            tooltip=['DATE:T', 'ANOMALY_TYPE', 'COUNT'],
        ).properties(height=300)
        st.altair_chart(line, use_container_width=True)

        # Detail table
        st.markdown("#### Anomaly Details")
        type_filter = st.multiselect(
            "Filter by anomaly type",
            options=sorted(anomaly_df['ANOMALY_TYPE'].unique()),
            default=sorted(anomaly_df['ANOMALY_TYPE'].unique()),
            key="anomaly_filter",
        )
        filtered = anomaly_df[anomaly_df['ANOMALY_TYPE'].isin(type_filter)]
        show_cols = ['RECORD_ID', 'ANOMALY_TYPE', 'DETAILS', 'DETECTED_AT']
        st.dataframe(
            filtered[show_cols].head(500),
            use_container_width=True,
        )
    else:
        st.info("No anomalies detected.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5 — PIPELINE LINEAGE & HEALTH
# ═══════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Pipeline Lineage & Health")

    # Pipeline layer distribution
    st.markdown("#### Tables by Pipeline Layer")
    layer_tags = tags_df[
        (tags_df['TAG_NAME'] == 'PIPELINE_LAYER') & (tags_df['DOMAIN'] == 'TABLE')
    ]
    if not layer_tags.empty:
        layer_dist = layer_tags.groupby('TAG_VALUE').size().reset_index(name='COUNT')
        layer_dist.columns = ['Layer', 'Tables']
        # Order Bronze > Silver > Gold
        layer_order = ['BRONZE', 'SILVER', 'GOLD']
        bar = alt.Chart(layer_dist).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X('Layer:N', sort=layer_order, title=None),
            y=alt.Y('Tables:Q', title='Table Count'),
            color=alt.Color('Layer:N', scale=alt.Scale(
                domain=layer_order,
                range=['#f59e0b', '#3b82f6', '#11b981'],
            ), legend=None),
            tooltip=['Layer', 'Tables'],
        ).properties(height=250)
        st.altair_chart(bar, use_container_width=True)

    # Data domain coverage
    st.markdown("#### Data Domain Coverage")
    domain_tags = tags_df[
        (tags_df['TAG_NAME'] == 'DATA_DOMAIN') & (tags_df['DOMAIN'] == 'TABLE')
    ]
    if not domain_tags.empty:
        domain_dist = domain_tags.groupby(['OBJECT_SCHEMA', 'TAG_VALUE']).size().reset_index(name='COUNT')
        domain_dist.columns = ['Schema', 'Domain', 'Tables']
        bar = alt.Chart(domain_dist).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X('Schema:N', title='Layer', sort=['BRONZE', 'SILVER', 'GOLD']),
            y=alt.Y('Tables:Q', title='Tables'),
            color=alt.Color('Schema:N', scale=alt.Scale(
                domain=['BRONZE', 'SILVER', 'GOLD'],
                range=['#f59e0b', '#3b82f6', '#11b981'],
            ), title='Layer'),
            tooltip=['Schema', 'Domain', 'Tables'],
        ).properties(height=300).facet(
            column=alt.Column('Domain:N', title='Data Domain'),
        )
        st.altair_chart(bar, use_container_width=True)

    st.markdown("---")

    # Row count consistency check
    st.markdown("#### Row Count Consistency (Bronze vs Silver vs Gold)")
    if not tables_df.empty:
        rc = tables_df[['TABLE_SCHEMA', 'TABLE_NAME', 'ROW_COUNT']].copy()
        rc.columns = ['Schema', 'Table', 'Rows']
        rc['Rows'] = rc['Rows'].fillna(0).astype(int)

        # Pivot by entity type
        entity_map = {
            'CUSTOMERS': ['CUSTOMERS', 'STG_CUSTOMERS', 'DIM_CUSTOMERS'],
            'PRODUCTS': ['PRODUCTS', 'STG_PRODUCTS', 'DIM_PRODUCTS'],
            'ORDERS': ['ORDERS', 'STG_ORDERS', 'FACT_SALES'],
        }
        consistency_rows = []
        for entity, table_names in entity_map.items():
            row = {'Entity': entity}
            for tn in table_names:
                match = rc[rc['Table'] == tn]
                if not match.empty:
                    schema = match.iloc[0]['Schema']
                    row[schema] = int(match.iloc[0]['Rows'])
            consistency_rows.append(row)

        cons_df = pd.DataFrame(consistency_rows)
        # Check if Bronze == Silver == Gold
        for col in ['BRONZE', 'SILVER', 'GOLD']:
            if col not in cons_df.columns:
                cons_df[col] = 0
        cons_df['Match'] = cons_df.apply(
            lambda r: 'Yes' if r['BRONZE'] == r['SILVER'] == r['GOLD'] else 'No', axis=1
        )
        st.dataframe(cons_df[['Entity', 'BRONZE', 'SILVER', 'GOLD', 'Match']],
                      use_container_width=True)

    st.markdown("---")

    # Full table inventory
    st.markdown("#### Full Table Inventory")
    if not tables_df.empty:
        inv = tables_df.copy()
        inv['ROW_COUNT'] = inv['ROW_COUNT'].fillna(0).astype(int)
        inv['SIZE_MB'] = (inv['BYTES'].fillna(0) / (1024 * 1024)).round(2)
        inv['LAST_ALTERED'] = pd.to_datetime(
            inv['LAST_ALTERED'].astype(str).str.strip('"')
        )
        display = inv[['TABLE_SCHEMA', 'TABLE_NAME', 'ROW_COUNT', 'SIZE_MB', 'LAST_ALTERED']]
        display.columns = ['Schema', 'Table', 'Rows', 'Size (MB)', 'Last Altered']
        st.dataframe(display, use_container_width=True)

    st.markdown("---")

    # Bronze profiling results
    st.markdown("#### Bronze Profiling Highlights")
    if not profile_df.empty:
        # Show null percentage metrics
        null_metrics = profile_df[profile_df['METRIC'] == 'null_pct'].copy()
        if not null_metrics.empty:
            null_metrics['METRIC_VALUE'] = pd.to_numeric(null_metrics['METRIC_VALUE'], errors='coerce')
            null_high = null_metrics[null_metrics['METRIC_VALUE'] > 0].sort_values(
                'METRIC_VALUE', ascending=False
            ).head(20)
            if not null_high.empty:
                null_display = null_high[['TABLE_NAME', 'COLUMN_NAME', 'METRIC_VALUE']].copy()
                null_display.columns = ['Table', 'Column', 'Null %']
                null_display['Null %'] = null_display['Null %'].round(2)
                st.dataframe(null_display, use_container_width=True)
            else:
                st.success("All Bronze columns have 0% nulls in profiling.")
        else:
            st.info("No null_pct profiling data available.")
    else:
        st.info("No profiling data available.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption("CoCo DE Governance Dashboard | Powered by Snowflake Tags, DMFs & Anomaly Detection")
