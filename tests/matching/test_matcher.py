import pytest
import pandas as pd
import logging
from matching.matcher import (
    reconcile,
    deterministic_match,
    fuzzy_match,
    get_unmatched,
    _similarity,
    _normalize
)

@pytest.fixture
def sample_data():
    """Provide simple client/internal datasets for testing."""
    client = pd.DataFrame([
        {"order_id": 1, "job_date": "2025-10-01", "site": "Alpha Plant", "service_type": "inspection"},
        {"order_id": 2, "job_date": "2025-10-02", "site": "Beta Plant", "service_type": "repair"},
        {"order_id": 3, "job_date": "2025-10-03", "site": "Gamma Plant", "service_type": "audit"},
    ])

    internal = pd.DataFrame([
        {"job_id": 101, "job_date": "2025-10-01", "site": "Alpha Plant", "service_type": "inspection"},
        {"job_id": 102, "job_date": "2025-10-02", "site": "Beta Plant", "service_type": "repair"},
        {"job_id": 103, "job_date": "2025-10-04", "site": "Delta Plant", "service_type": "audit"},
    ])
    return client, internal

@pytest.fixture
def fuzzy_data():
    """Data to test fuzzy matching with minor text variations."""
    client = pd.DataFrame([
        {"order_id": 1, "job_date": "2025-10-01", "site": "Alfa Plant", "service_type": "inspection"},
        {"order_id": 2, "job_date": "2025-10-02", "site": "Beta Plnt", "service_type": "repair"},
    ])
    internal = pd.DataFrame([
        {"job_id": 101, "job_date": "2025-10-01", "site": "Alpha Plant", "service_type": "inspection"},
        {"job_id": 102, "job_date": "2025-10-02", "site": "Beta Plant", "service_type": "repair"},
    ])
    return client, internal

@pytest.mark.parametrize(
    "a,b,expected",
    [
        ("alpha", "alpha", 1.0),
        ("alpha", "alph", pytest.approx(0.89, rel=0.1)),
        ("", "alpha", 0.0),
        (None, "alpha", 0.0),
    ],
)
def test_similarity(a, b, expected):
    assert _similarity(a, b) == pytest.approx(expected, rel=0.1)

@pytest.mark.parametrize(
    "value,expected",
    [
        (" Alpha ", "alpha"),
        ("BETA", "beta"),
        (None, ""),
        (123, ""),
    ],
)
def test_normalize(value, expected):
    assert _normalize(value) == expected

def test_deterministic_match_exact(sample_data):
    client, internal = sample_data
    result = deterministic_match(client, internal, ["job_date", "site", "service_type"])
    assert not result.empty
    assert all(result["match_type"] == "deterministic")
    assert all(result["confidence"] == 1.0)
    expected_order_ids = {1, 2}
    assert set(result["order_id"]) == expected_order_ids

def test_deterministic_match_no_keys(sample_data):
    client, internal = sample_data
    with pytest.raises(ValueError):
        deterministic_match(client, internal, [])

def test_fuzzy_match_typo_detection(fuzzy_data):
    client, internal = fuzzy_data
    client["site"] = client["site"].apply(_normalize)
    internal["site"] = internal["site"].apply(_normalize)
    result = fuzzy_match(client, internal, threshold=0.8)
    assert not result.empty
    assert all(result["match_type"] == "fuzzy")
    assert set(result["order_id"]) == {1, 2}

def test_fuzzy_match_empty_dataframe_returns_empty():
    empty = pd.DataFrame()
    result = fuzzy_match(empty, empty)
    assert result.empty

def test_fuzzy_match_above_threshold_no_match(sample_data):
    client, internal = sample_data
    client.loc[0, "site"] = "CompletelyDifferent"
    result = fuzzy_match(client.iloc[[0]], internal, threshold=0.99)
    assert result.empty

def test_get_unmatched_returns_remaining(sample_data):
    client, internal = sample_data
    matched = deterministic_match(client, internal, ["job_date", "site", "service_type"])
    unmatched = get_unmatched(client, matched, ["job_date", "site", "service_type"])
    expected_unmatched = set(client["order_id"]) - set(matched["order_id"])
    assert set(unmatched["order_id"]) == expected_unmatched

def test_get_unmatched_with_empty_matched(sample_data):
    client, _ = sample_data
    unmatched = get_unmatched(client, pd.DataFrame(), ["job_date"])
    pd.testing.assert_frame_equal(unmatched, client)

def test_reconcile_combines_matches(sample_data):
    client, internal = sample_data
    all_matches, client_unmatched, internal_unmatched = reconcile(client, internal, threshold=0.7)
    assert isinstance(all_matches, pd.DataFrame)
    assert "confidence" in all_matches.columns
    assert all(col in all_matches.columns for col in ["match_type", "site"])
    assert isinstance(client_unmatched, pd.DataFrame)
    assert isinstance(internal_unmatched, pd.DataFrame)
    assert len(all_matches) + len(client_unmatched) <= len(client)

def test_reconcile_empty_inputs_raises():
    with pytest.raises(ValueError):
        reconcile(pd.DataFrame(), pd.DataFrame())

def test_reconcile_no_common_columns():
    c = pd.DataFrame({"x": [1], "y": [2]})
    i = pd.DataFrame({"a": [1], "b": [2]})
    with pytest.raises(KeyError):
        reconcile(c, i)
