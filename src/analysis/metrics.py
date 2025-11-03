"""
metrics.py
-----------
Computes reconciliation metrics, variance thresholds, and anomaly summaries.

- Per-row revenue variance (<1% tolerance by default)
- Summarizes matched/unmatched jobs
- Detects anomalies: rate_change, duplicate, missing_job, new_job, unit_mismatch
"""

import pandas as pd
import numpy as np
from typing import Dict
from utils.logger_config import get_logger

logger = get_logger(__name__)

def _resolve_revenue_columns(df: pd.DataFrame):
    """Detect client/internal revenue columns after merge."""
    logger.debug("Resolving revenue columns from DataFrame")
    client_col, internal_col = None, None
    for c in df.columns:
        if "amount" in c or (c.endswith("_client") and "revenue" in c):
            client_col = c
        if "revenue" in c and c.endswith("_internal"):
            internal_col = c

    if client_col is None and "amount" in df.columns:
        client_col = "amount"
    if internal_col is None and "revenue" in df.columns:
        internal_col = "revenue"

    if not client_col or not internal_col:
        logger.error(f"Could not detect revenue columns. Available: {df.columns.tolist()}")
        raise KeyError(f"Cannot detect revenue columns. Available: {df.columns.tolist()}")

    logger.info(f"Detected revenue columns → client: {client_col}, internal: {internal_col}")
    return client_col, internal_col


def calculate_revenue_variance(matched_df: pd.DataFrame, tolerance: float = 1.0) -> pd.DataFrame:
    """Compute per-row revenue variance and tolerance flag."""
    if matched_df.empty:
        logger.warning("Received empty DataFrame for variance calculation.")
        return matched_df

    logger.info(f"Calculating revenue variance for {len(matched_df)} matched rows (tolerance={tolerance}%)")
    client_col, internal_col = _resolve_revenue_columns(matched_df)
    revenue_delta = abs(np.floor(matched_df[client_col]) - np.floor(matched_df[internal_col]))
    avg_sum = (np.floor(matched_df[client_col]) + np.floor(matched_df[internal_col])) / 2
    pct_variance = (revenue_delta / avg_sum) * 100
    within_tolerance = pct_variance <= tolerance  # inclusive

    variance_df = matched_df.assign(
        revenue_delta=revenue_delta,
        pct_variance=pct_variance,
        within_tolerance=within_tolerance
    )

    out_of_tolerance = (~within_tolerance).sum()
    if out_of_tolerance > 0:
        logger.warning(f"{out_of_tolerance} rows exceed {tolerance:.2f}% tolerance threshold.")

    return variance_df


def summarize_metrics(matched_df: pd.DataFrame, unmatched_client: pd.DataFrame,
                      unmatched_internal: pd.DataFrame, tolerance: float = 1.0) -> Dict:
    """Summarize reconciliation KPIs for dashboard."""
    logger.info("Generating summary metrics for reconciliation results.")
    if matched_df.empty:
        logger.warning("No matched records — returning empty metrics summary.")
        total_jobs = len(unmatched_client) + len(unmatched_internal)
        return {
            "total_jobs": total_jobs,
            "matched_jobs": 0,
            "match_rate": 0,
            "avg_variance_pct": 0,
            "within_tolerance_pct": 0,
            "unmatched_client_jobs": len(unmatched_client),
            "unmatched_internal_jobs": len(unmatched_internal),
            "total_variance_amount": 0,
        }

    total_jobs = max(len(matched_df) + len(unmatched_client), len(matched_df) + len(unmatched_internal))
    variance_df = calculate_revenue_variance(matched_df, tolerance=tolerance)
    avg_variance = variance_df["pct_variance"].mean()
    within_tolerance = variance_df["within_tolerance"].mean() * 100

    metrics = {
        "total_jobs": total_jobs,
        "matched_jobs": len(matched_df),
        "match_rate": round(len(matched_df)/total_jobs*100,2),
        "avg_variance_pct": round(avg_variance,2),
        "within_tolerance_pct": round(within_tolerance,2),
        "unmatched_client_jobs": len(unmatched_client),
        "unmatched_internal_jobs": len(unmatched_internal),
        "total_variance_amount": round(variance_df["revenue_delta"].sum(),2)
    }

    logger.info(
        f"Summary metrics → Match rate: {metrics['match_rate']}%, "
        f"Avg variance: {metrics['avg_variance_pct']}%, "
        f"Unmatched: C={metrics['unmatched_client_jobs']} / I={metrics['unmatched_internal_jobs']}"
    )

    return metrics


def detect_anomalies(matched_df: pd.DataFrame, tolerance: float = 1.0) -> pd.DataFrame:
    """Detect anomalies and add detailed explanation."""
    logger.info("Detecting anomalies in matched records.")
    if matched_df.empty:
        logger.warning("No data available for anomaly detection.")
        return matched_df

    df = calculate_revenue_variance(matched_df, tolerance=tolerance)
    df["anomaly"] = None
    df["anomaly_reason"] = None

    # Flags
    rate_change_mask = df["pct_variance"] > tolerance
    duplicate_mask = df.duplicated(subset=["job_date", "site"], keep=False)
    missing_client_mask = df.get("order_id_client", pd.Series([False]*len(df))).isna()
    missing_internal_mask = df.get("job_id_internal", pd.Series([False]*len(df))).isna()

    df.loc[rate_change_mask, "anomaly"] = "rate_change"
    df.loc[rate_change_mask, "anomaly_reason"] = df.apply(
        lambda r: f"Client vs Internal revenue differs by {r['pct_variance']:.2f}% (${r['revenue_delta']})", axis=1
    )

    df.loc[duplicate_mask, "anomaly"] = "duplicate"
    df.loc[duplicate_mask, "anomaly_reason"] = "Multiple entries for same site/date"

    df.loc[missing_client_mask, "anomaly"] = "missing_job"
    df.loc[missing_client_mask, "anomaly_reason"] = "Job missing from client data"

    df.loc[missing_internal_mask, "anomaly"] = "new_job"
    df.loc[missing_internal_mask, "anomaly_reason"] = "Job present in internal ledger but missing from client data"

    anomaly_counts = df["anomaly"].value_counts(dropna=True).to_dict()
    if anomaly_counts:
        logger.warning(f"Detected anomalies: {anomaly_counts}")
    else:
        logger.info("No anomalies detected within the given tolerance.")

    return df
