import streamlit as st
import pandas as pd
# from pathlib import Path
from matching.matcher import reconcile
from analysis.metrics import summarize_metrics, detect_anomalies, calculate_revenue_variance
from data.fetcher import load_client_data, load_internal_data
from data.normalizer import normalize_dataframe

st.set_page_config(
    page_title="Revenue Reconciliation & Anomaly Detection",
    layout="wide",
    page_icon="📊"
)

st.title("Revenue Reconciliation & Anomaly Detection")
st.markdown("Detect and explain differences between **client job data** and **internal ledger**.")

st.sidebar.header("Configuration")
auto_load = st.sidebar.checkbox("Use sample data", value=True)
tolerance = st.sidebar.number_input("Variance tolerance (%)", 0.1, 5.0, 1.0, step=0.1)
st.sidebar.divider()

# Load data
if auto_load:
    client_df = normalize_dataframe(load_client_data())
    ledger_df = normalize_dataframe(load_internal_data())
else:
    st.sidebar.markdown("### Upload CSVs")
    client_file = st.sidebar.file_uploader("Client data (CSV)", type=["csv"])
    ledger_file = st.sidebar.file_uploader("Internal ledger (CSV)", type=["csv"])
    if client_file and ledger_file:
        client_df = pd.read_csv(client_file)
        ledger_df = pd.read_csv(ledger_file)
    else:
        st.warning("Please upload both client and ledger files or enable sample data.")
        st.stop()

# View raw data
with st.expander("View Raw Data"):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Client Data")
        st.dataframe(client_df.head())
    with c2:
        st.subheader("Internal Ledger")
        st.dataframe(ledger_df.head())

# Reconciliation
try:
    matched_df, unmatched_client, unmatched_internal = reconcile(client_df, ledger_df)
except ValueError as e:
    st.error(f"Matching failed: {e}")
    st.stop()

# Clean matched dataframe
# matched_df = clean_matched_df(matched_df)

# Metrics & variance
st.header("Metrics & Variance Summary")
metrics = summarize_metrics(matched_df, unmatched_client, unmatched_internal, tolerance=tolerance)
st.write(metrics)
col1, col2, col3 = st.columns(3)
col1.metric("Matched Jobs", f"{metrics['matched_jobs']}")
col2.metric("Match Rate", f"{metrics['match_rate']:.0f}%")
col3.metric("Avg Variance", f"{metrics['avg_variance_pct']:.2f}%")

col4, col5, col6 = st.columns(3)
col4.metric("Unmatched Client Jobs", f"{metrics['unmatched_client_jobs']}")
col5.metric("Unmatched Internal Jobs", f"{metrics['unmatched_internal_jobs']}")
col6.metric("Total difference Variance", f"${metrics['total_variance_amount']:.0f}")

# Rows that exceed tolerance
matched_df = calculate_revenue_variance(matched_df, tolerance=tolerance)
exceptions_df = matched_df[~matched_df["within_tolerance"]]

if not exceptions_df.empty:
    st.error(f"{len(exceptions_df)} matched rows exceed {tolerance:.1f}% variance tolerance! Review required.")
else:
    st.success(f"All matched rows are within {tolerance:.1f}% variance tolerance.")

# Anomaly detection
st.header("Anomaly Classification")
anomalies_df = detect_anomalies(matched_df, tolerance=tolerance)
st.write(anomalies_df[anomalies_df["anomaly"].notna()].head(50))

# Footer
st.markdown("---")
st.caption("Built for AI Solutions Analyst Assessment, Revenue Reconciliation & Anomaly Detection (2025)")