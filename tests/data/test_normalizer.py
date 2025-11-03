import pytest
import pandas as pd
from data.normalizer import (
    _normalize_text,
    _normalize_service_type,
    normalize_dataframe,
)

# ---------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------
@pytest.mark.parametrize(
    "input_text,expected",
    [
        (" 123 Main St. ", "123 Main Street"),
        ("5th ave", "5Th Avenue"),
        ("Hwy 99 Blvd", "Highway 99 Boulevard"),
        ("Plz Central", "Plaza Central"),
        ("NORMAL TEXT", "Normal Text"),
        (123, ""),      # Non-string input
        (None, ""),     # None input
    ]
)
def test_normalize_text(input_text, expected):
    assert _normalize_text(input_text) == expected


@pytest.mark.parametrize(
    "input_type,expected",
    [
        ("inspection", "INSPECTION"),
        ("  repair  ", "REPAIR"),
        ("Audit", "AUDIT"),
        (123, ""),       # Non-string input
        (None, ""),      # None input
    ]
)
def test_normalize_service_type(input_type, expected):
    assert _normalize_service_type(input_type) == expected


# ---------------------------------------------------------------------
# Tests for normalize_dataframe
# ---------------------------------------------------------------------
def test_normalize_dataframe_basic(caplog):
    df = pd.DataFrame([
        {"job_date": "2025-10-01", "site": "123 Main St.", "service_type": "inspection"},
        {"job_date": "2025-10-02", "site": "5th ave", "service_type": "repair"},
    ])

    caplog.set_level("INFO")
    normalized = normalize_dataframe(df)

    # Job date converted to datetime
    assert pd.api.types.is_datetime64_any_dtype(normalized["job_date"])

    # Site normalized
    assert normalized.loc[0, "site"] == "123 Main Street"
    assert normalized.loc[1, "site"] == "5Th Avenue"

    # Service type normalized
    assert normalized.loc[0, "service_type"] == "INSPECTION"
    assert normalized.loc[1, "service_type"] == "REPAIR"

    # Logging captured
    assert any("Starting normalization" in msg for msg in caplog.messages)
    assert any("Normalization complete" in msg for msg in caplog.messages)


def test_normalize_dataframe_empty(caplog):
    df = pd.DataFrame()
    caplog.set_level("WARNING")
    normalized = normalize_dataframe(df)
    assert normalized.empty
    assert any("Received empty DataFrame" in msg for msg in caplog.messages)


def test_normalize_dataframe_missing_columns():
    df = pd.DataFrame([{"job_date": "2025-10-01", "site": "123 Main St."}])
    with pytest.raises(KeyError) as excinfo:
        normalize_dataframe(df)
    assert "Missing columns" in str(excinfo.value)


def test_normalize_dataframe_invalid_dates(caplog):
    df = pd.DataFrame([
        {"job_date": "invalid", "site": "123 Main St.", "service_type": "inspection"},
        {"job_date": None, "site": "5th ave", "service_type": "repair"},
    ])
    caplog.set_level("WARNING")
    normalized = normalize_dataframe(df)
    # Invalid dates coerced to NaT
    assert normalized["job_date"].isna().sum() == 2
    assert any("invalid or missing 'job_date'" in msg for msg in caplog.messages)


def test_normalize_dataframe_preserves_rows_and_columns():
    df = pd.DataFrame([
        {"job_date": "2025-10-01", "site": "123 Main St.", "service_type": "inspection", "extra": 1}
    ])
    normalized = normalize_dataframe(df)
    # Extra column preserved
    assert "extra" in normalized.columns
    assert normalized.shape[0] == 1
