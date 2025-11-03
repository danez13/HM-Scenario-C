import pytest
import pandas as pd
from pathlib import Path
from fetcher import load_client_data, load_internal_data, fetch_from_api, _simulate_api_delay

# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------
@pytest.fixture
def sample_client_df():
    return pd.DataFrame([
        {"order_id": 1, "job_date": "2025-10-01", "site": "Site A", "service_type": "inspection"}
    ])


@pytest.fixture
def sample_internal_df():
    return pd.DataFrame([
        {"job_id": 101, "job_date": "2025-10-01", "site": "Site A", "service_type": "inspection"}
    ])

# ---------------------------------------------------------------------
# Test fetch_from_api (mocking filesystem)
# ---------------------------------------------------------------------
def test_fetch_from_api_success(monkeypatch, tmp_path, sample_client_df):
    # Create a temporary CSV to simulate API fetch
    client_csv = tmp_path / "client_data.csv"
    sample_client_df.to_csv(client_csv, index=False)

    # Patch DATA_DIR to tmp_path
    monkeypatch.setattr("fetcher.DATA_DIR", tmp_path)

    df = fetch_from_api("https://api.mockclientdata.local/jobs")
    pd.testing.assert_frame_equal(df, sample_client_df)


def test_fetch_from_api_file_not_found(monkeypatch, tmp_path):
    monkeypatch.setattr("fetcher.DATA_DIR", tmp_path)
    df = fetch_from_api("https://api.mockclientdata.local/jobs")
    assert df.empty


# ---------------------------------------------------------------------
# Test load_client_data
# ---------------------------------------------------------------------
def test_load_client_data_api(monkeypatch, sample_client_df):
    # Mock fetch_from_api
    monkeypatch.setattr("fetcher.fetch_from_api", lambda url: sample_client_df)
    df = load_client_data(source="api")
    pd.testing.assert_frame_equal(df, sample_client_df)


def test_load_client_data_csv(monkeypatch, tmp_path, sample_client_df):
    # Create a fake CSV file
    csv_path = tmp_path / "client_jobs.csv"
    sample_client_df.to_csv(csv_path, index=False)

    monkeypatch.setattr("fetcher.DATA_DIR", tmp_path)
    df = load_client_data(source="csv")
    pd.testing.assert_frame_equal(df, sample_client_df)


def test_load_client_data_sample(monkeypatch, sample_client_df):
    monkeypatch.setattr("fetcher.fetch_from_api", lambda url: sample_client_df)
    df = load_client_data(source="sample")
    pd.testing.assert_frame_equal(df, sample_client_df)


# ---------------------------------------------------------------------
# Test load_internal_data
# ---------------------------------------------------------------------
def test_load_internal_data_api(monkeypatch, sample_internal_df):
    monkeypatch.setattr("fetcher.fetch_from_api", lambda url: sample_internal_df)
    df = load_internal_data(source="api")
    pd.testing.assert_frame_equal(df, sample_internal_df)


def test_load_internal_data_csv(monkeypatch, tmp_path, sample_internal_df):
    csv_path = tmp_path / "internal_ledger.csv"
    sample_internal_df.to_csv(csv_path, index=False)

    monkeypatch.setattr("fetcher.DATA_DIR", tmp_path)
    df = load_internal_data(source="csv")
    pd.testing.assert_frame_equal(df, sample_internal_df)


def test_load_internal_data_sample(monkeypatch, sample_internal_df):
    monkeypatch.setattr("fetcher.fetch_from_api", lambda url: sample_internal_df)
    df = load_internal_data(source="sample")
    pd.testing.assert_frame_equal(df, sample_internal_df)


# ---------------------------------------------------------------------
# Test _simulate_api_delay runs without error
# ---------------------------------------------------------------------
def test_simulate_api_delay_runs(monkeypatch):
    # Patch time.sleep to avoid slowing tests
    monkeypatch.setattr("time.sleep", lambda x: None)
    _simulate_api_delay()  # Should run without exceptions
