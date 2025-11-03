"""
app.py
------
Streamlit application for Revenue Reconciliation & Anomaly Detection.

Logs:
 - Data loading, normalization, and reconciliation steps
 - Anomaly detection and user interactions
 - Errors and performance metrics

Author: AI Solutions Analyst Assessment (2025)
"""

import streamlit as st
import pandas as pd
import os
import time
import logging
from matching.matcher import reconcile
from analysis.metrics import summarize_metrics, detect_anomalies, calculate_revenue_variance
from data.fetcher import load_client_data, load_internal_data
from data.normalizer import normalize_dataframe

from utils.logger_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------
# Streamlit Setup
# ---------------------------------------------------------------------
st.set_page_config(page_title="Revenue Reconciliation & Anomaly Detection", layout="wide", page_icon="📊")
st.title("Revenue Reconciliation & Anomaly Detection")
st.markdown("Detect and explain differences between **client job data** and **internal ledger**.")

logger.info("Streamlit app started successfully")

# ---------------------------------------------------------------------
# Sidebar Configuration
# ---------------------------------------------------------------------
st.sidebar.header("Configuration")
auto_load = st.sidebar.checkbox("Use sample data", value=True)
tolerance = st.sidebar.number_input("Variance tolerance (%)", 0.1, 5.0, 1.0, step=0.1)
st.sidebar.divider()
logger.info(f"Sidebar settings — auto_load={auto_load}, tolerance={tolerance:.2f}%")

# ---------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------
start_load_time = time.time()

try:
    if auto_load:
        logger.info("Loading sample datasets via fetcher...")
        client_df = normalize_dataframe(load_client_data())
        ledger_df = normalize_dataframe(load_internal_data())
        logger.info(f"Loaded sample data: client={len(client_df)} rows, ledger={len(ledger_df)} rows")
    else:
        st.sidebar.markdown("### Upload CSVs")
        client_file = st.sidebar.file_uploader("Client data (CSV)", type=["csv"])
        ledger_file = st.sidebar.file_uploader("Internal ledger (CSV)", type=["csv"])

        if client_file and ledger_file:
            client_df = pd.read_csv(client_file)
            ledger_df = pd.read_csv(ledger_file)
            logger.info(f"Loaded uploaded CSVs: client={len(client_df)} rows, ledger={len(ledger_df)} rows")
        else:
            st.warning("Please upload both client and ledger files or enable sample data.")
            logger.warning("User stopped due to missing uploads.")
            st.stop()

    load_duration = time.time() - start_load_time
    logger.info(f"Data load completed in {load_duration:.2f}s")

except Exception as e:
    logger.exception(f"Data loading failed: {e}")
    st.error("Failed to load data. Check logs for details.")
    st.stop()

# ---------------------------------------------------------------------
# Show Raw Data
# ---------------------------------------------------------------------
with st.expander("View Raw Data"):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Client Data")
        st.dataframe(client_df.head())
    with c2:
        st.subheader("Internal Ledger")
        st.dataframe(ledger_df.head())

logger.info("Displayed raw data preview in expander section.")

# ---------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------
try:
    logger.info("Starting deterministic + fuzzy reconciliation process...")
    matched_df, unmatched_client, unmatched_internal = reconcile(client_df, ledger_df)
    logger.info(f"Reconciliation complete — matched={len(matched_df)}, "
                f"unmatched_client={len(unmatched_client)}, unmatched_internal={len(unmatched_internal)}")

except ValueError as e:
    logger.exception(f"Reconciliation failed: {e}")
    st.error(f"Matching failed: {e}")
    st.stop()

# ---------------------------------------------------------------------
# Metrics & Variance
# ---------------------------------------------------------------------
logger.info("Computing metrics and variance summary...")
st.header("Metrics & Variance Summary")

metrics = summarize_metrics(matched_df, unmatched_client, unmatched_internal, tolerance=tolerance)
logger.info(f"Metrics calculated: {metrics}")

cols = st.columns(3)
cols[0].metric("Matched Jobs", metrics["matched_jobs"])
cols[1].metric("Match Rate", f"{metrics['match_rate']:.0f}%")
cols[2].metric("Avg Variance", f"{metrics['avg_variance_pct']:.2f}%")

cols = st.columns(3)
cols[0].metric("Unmatched Client Jobs", metrics["unmatched_client_jobs"])
cols[1].metric("Unmatched Internal Jobs", metrics["unmatched_internal_jobs"])
cols[2].metric("Total Difference Variance", f"${metrics['total_variance_amount']:.0f}")

# ---------------------------------------------------------------------
# Tolerance Enforcement
# ---------------------------------------------------------------------
matched_df = calculate_revenue_variance(matched_df, tolerance=tolerance)
exceptions_df = matched_df[~matched_df["within_tolerance"]]

if not exceptions_df.empty:
    logger.warning(f"{len(exceptions_df)} matched rows exceed {tolerance:.1f}% tolerance.")
    st.error(f"{len(exceptions_df)} matched rows exceed {tolerance:.1f}% variance tolerance!")
    st.header("Anomaly Classification & Review")

    anomalies_df = detect_anomalies(matched_df, tolerance=tolerance)
    anomalies_df["review_status"] = "pending"
    logger.info(f"Detected anomalies: {len(anomalies_df)} rows flagged.")

    # Filter interface
    anomaly_filter = st.multiselect(
        "Filter anomalies by type:",
        options=anomalies_df["anomaly"].dropna().unique(),
        default=anomalies_df["anomaly"].dropna().unique()
    )
    logger.info(f"Active anomaly filters: {anomaly_filter}")

    if anomaly_filter:
        display_df = anomalies_df[anomalies_df["anomaly"].isin(anomaly_filter)]
    else:
        display_df = anomalies_df.dropna(subset=["anomaly"]).reset_index(drop=True)

    # Editable anomaly table
    edited_anomalies = st.data_editor(
        display_df,
        num_rows="dynamic",
        column_config={
            "anomaly": st.column_config.SelectboxColumn(
                "Anomaly Type", options=["rate_change", "duplicate", "missing_job", "new_job", "unit_mismatch"]
            ),
            "review_status": st.column_config.SelectboxColumn(
                "Review Status", options=["pending", "approved", "rejected"]
            ),
            "anomaly_reason": st.column_config.TextColumn("Reason", help="Explains why anomaly was flagged")
        }
    )

    logger.info(f"User edited anomalies: {len(edited_anomalies)} records displayed for review.")

    st.markdown("### Summary by Anomaly Type")
    summary = edited_anomalies.groupby("anomaly").agg(
        count=("anomaly", "count")
    ).reset_index()
    st.dataframe(summary, hide_index=True)

else:
    st.success(f"All matched rows are within {tolerance:.1f}% variance tolerance.")
    logger.info("All records within variance tolerance.")

# ---------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------
st.markdown("---")
st.caption("Built for AI Solutions Analyst Assessment — Revenue Reconciliation & Anomaly Detection (2025)")
logger.info("App execution complete.")
