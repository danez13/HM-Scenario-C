import pytest
import pandas as pd
import numpy as np
from analysis.metrics import (
    _resolve_revenue_columns,
    calculate_revenue_variance,
    summarize_metrics,
    detect_anomalies
)

@pytest.fixture
def matched_df():
    return pd.DataFrame([
        {"order_id_client": 1, "job_id_internal": 101, "job_date": "2025-10-01", "site": "Site A",
         "service_type": "inspection", "amount_client": 100.0, "revenue_internal": 101.0},
        {"order_id_client": 2, "job_id_internal": 102, "job_date": "2025-10-02", "site": "Site B",
         "service_type": "repair", "amount_client": 200.0, "revenue_internal": 198.0},
        {"order_id_client": 3, "job_id_internal": 103, "job_date": "2025-10-03", "site": "Site C",
         "service_type": "audit", "amount_client": 300.0, "revenue_internal": 300.0},
    ])


@pytest.fixture
def unmatched_client():
    return pd.DataFrame([
        {"order_id_client": 4, "job_date": "2025-10-04", "site": "Site D"}
    ])


@pytest.fixture
def unmatched_internal():
    return pd.DataFrame([
        {"job_id_internal": 104, "job_date": "2025-10-05", "site": "Site E"}
    ])

def test_resolve_revenue_columns_detects_columns(matched_df):
    client_col, internal_col = _resolve_revenue_columns(matched_df)
    assert client_col in ["amount_client", "amount_client"]  # our column name
    assert internal_col in ["revenue_internal", "revenue_internal"]


def test_resolve_revenue_columns_missing_columns():
    df = pd.DataFrame([{"job_date": "2025-10-01"}])
    with pytest.raises(KeyError):
        _resolve_revenue_columns(df)

def test_calculate_revenue_variance_basic(matched_df):
    result = calculate_revenue_variance(matched_df, tolerance=1.0)
    assert "revenue_delta" in result.columns
    assert "pct_variance" in result.columns
    assert "within_tolerance" in result.columns

    # Compute expected manually
    client_col, internal_col = "amount_client", "revenue_internal"
    revenue_delta = abs(np.floor(matched_df[client_col]) - np.floor(matched_df[internal_col]))
    avg_sum = (np.floor(matched_df[client_col]) + np.floor(matched_df[internal_col])) / 2
    pct_variance = (revenue_delta / avg_sum) * 100

    expected = (pct_variance <= 1.0).rename("within_tolerance")  # <-- give the correct name
    pd.testing.assert_series_equal(result["within_tolerance"], expected)




def test_calculate_revenue_variance_empty():
    df = pd.DataFrame()
    result = calculate_revenue_variance(df)
    assert result.empty

def test_summarize_metrics_basic(matched_df, unmatched_client, unmatched_internal):
    metrics = summarize_metrics(matched_df, unmatched_client, unmatched_internal, tolerance=1.0)
    assert metrics["matched_jobs"] == len(matched_df)
    assert metrics["unmatched_client_jobs"] == len(unmatched_client)
    assert metrics["unmatched_internal_jobs"] == len(unmatched_internal)
    assert "avg_variance_pct" in metrics
    assert "within_tolerance_pct" in metrics


def test_summarize_metrics_empty(unmatched_client, unmatched_internal):
    metrics = summarize_metrics(pd.DataFrame(), unmatched_client, unmatched_internal)
    assert metrics["matched_jobs"] == 0
    assert metrics["total_jobs"] == len(unmatched_client) + len(unmatched_internal)
    assert metrics["match_rate"] == 0

def test_detect_anomalies_flags(matched_df):
    df = detect_anomalies(matched_df, tolerance=1.0)
    assert "anomaly" in df.columns
    assert "anomaly_reason" in df.columns

    # Ensure at least one rate_change anomaly exists
    assert "rate_change" in df["anomaly"].values

    # Optionally, check anomaly_reason contains expected text
    rate_change_rows = df[df["anomaly"] == "rate_change"]
    for _, row in rate_change_rows.iterrows():
        assert "Client vs Internal revenue differs" in row["anomaly_reason"]



def test_detect_anomalies_empty():
    df = pd.DataFrame()
    result = detect_anomalies(df)
    assert result.empty
