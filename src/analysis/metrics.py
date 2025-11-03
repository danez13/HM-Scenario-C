"""
metrics.py
-----------
Computes reconciliation metrics, variance thresholds, and anomaly summaries.

Responsibilities:
- Measure total and percentage deltas between client and internal systems
- Apply 1% tolerance threshold for revenue variance
- Summarize anomalies (rate changes, missing jobs, duplicates)
- Generate KPIs for dashboards or alerts
"""

import pandas as pd
from typing import Dict
import numpy as np

def _resolve_revenue_columns(df: pd.DataFrame):
    """
    Dynamically detect which columns represent client and internal revenues.
    """
    client_col = None
    internal_col = None

    # Common naming patterns after merge
    for c in df.columns:
        if "amount" in c or c.endswith("_client") and "revenue" in c:
            client_col = c
        if "revenue" in c and c.endswith("_internal"):
            internal_col = c

    # Fallbacks for pre-merge column names
    if client_col is None and "amount" in df.columns:
        client_col = "amount"
    if internal_col is None and "revenue" in df.columns:
        internal_col = "revenue"

    if not client_col or not internal_col:
        raise KeyError(
            f"Could not determine client/internal revenue columns. "
            f"Available: {df.columns.tolist()}"
        )

    return client_col, internal_col


def calculate_revenue_variance(matched_df: pd.DataFrame, tolerance: float = 1.0) -> pd.DataFrame:
    """
    Computes per-row and total variance between client and internal records.

    Dynamically detects correct column names.
    """
    if matched_df.empty:
        return matched_df

    client_col, internal_col = _resolve_revenue_columns(matched_df)

    revenue_delta = abs(np.floor(matched_df[client_col]) - np.floor(matched_df[internal_col]))
    avg_sum = (np.floor(matched_df[client_col]) + np.floor(matched_df[internal_col])) / 2
    pct_variance = (
        revenue_delta / avg_sum
    ) * 100

    within_tolerance = pct_variance < tolerance
    
    return pd.DataFrame({
        **matched_df,
        "revenue_delta": revenue_delta,
        "pct_variance": pct_variance,
        "within_tolerance": within_tolerance
    })

def summarize_metrics(
    matched_df: pd.DataFrame,
    unmatched_client: pd.DataFrame,
    unmatched_internal: pd.DataFrame,
    tolerance: float = 1.0
) -> Dict[str, float]:
    """
    Summarizes high-level reconciliation performance metrics.
    """
    if matched_df.empty:
        return {
            "total_jobs": len(unmatched_client) + len(unmatched_internal),
            "matched_jobs": 0,
            "match_rate": 0,
            "avg_variance_pct": 0,
            "within_tolerance_pct": 0,
            "unmatched_client_jobs": len(unmatched_client),
            "unmatched_internal_jobs": len(unmatched_internal),
            "total_variance_amount": 0,
        }

    total_client_jobs = len(matched_df) + len(unmatched_client)
    total_internal_jobs = len(matched_df) + len(unmatched_internal)
    total_jobs = max(total_client_jobs, total_internal_jobs)

    variance_df = calculate_revenue_variance(matched_df, tolerance=tolerance)
    avg_variance = variance_df["pct_variance"].mean()
    within_tolerance = variance_df["within_tolerance"].mean() * 100

    metrics = {
        "total_jobs": total_jobs,
        "matched_jobs": len(matched_df),
        "match_rate": (len(matched_df) / total_jobs) * 100 if total_jobs else 0,
        "avg_variance_pct": round(avg_variance, 2),
        "within_tolerance_pct": round(within_tolerance, 2),
        "unmatched_client_jobs": len(unmatched_client),
        "unmatched_internal_jobs": len(unmatched_internal),
        "total_variance_amount": round(variance_df["revenue_delta"].sum(), 2),
    }
    return metrics


def detect_anomalies(matched_df: pd.DataFrame, tolerance: float = 1.0) -> pd.DataFrame:
    """
    Identifies potential root causes for anomalies:
      - rate_change: same job keys, different rate
      - duplicate: multiple client entries for same site/date
    """
    if matched_df.empty:
        return matched_df

    df = calculate_revenue_variance(matched_df, tolerance=tolerance)
    df["anomaly"] = None

    rate_change_mask = (abs(df["revenue_delta"]) > 0) & (df["pct_variance"] > 1)
    duplicate_mask = df.duplicated(subset=["job_date", "site"], keep=False)

    df.loc[rate_change_mask, "anomaly"] = "rate_change"
    df.loc[duplicate_mask, "anomaly"] = "duplicate"
    return df
