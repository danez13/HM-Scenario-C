"""
fetcher.py
-----------
Handles data ingestion for both external (client) and internal (ledger) sources.

Supports:
 - API fetching (simulated for demo)
 - CSV fallback or sample data
 - Basic normalization (dates, site names, service type)
 - Error handling and logging

Author: AI Solutions Analyst Assessment (2025)
"""

import os
import time
import random
import pandas as pd
from pathlib import Path

from utils.logger_config import get_logger

logger = get_logger(__name__)


# Environment variables or defaults
CLIENT_API_URL = os.getenv("CLIENT_API_URL", "https://api.mockclientdata.local/jobs")
LEDGER_API_URL = os.getenv("LEDGER_API_URL", "https://api.internalledger.local/records")

DATA_DIR = Path("data")

# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------
def _simulate_api_delay():
    """Simulate network delay for realism."""
    delay = random.uniform(0.1, 0.4)
    time.sleep(delay)
    logger.debug(f"Simulated network delay: {delay:.2f}s")

# ---------------------------------------------------------------------
# Core Data Fetching
# ---------------------------------------------------------------------
def fetch_from_api(api_url: str) -> pd.DataFrame:
    """
    Simulated API call returning JSON job data or CSV-based mock.
    In production, this would use requests.get(api_url) and validate the response.
    """
    start_time = time.time()
    logger.info(f"Fetching data from API endpoint: {api_url}")
    _simulate_api_delay()

    try:
        if "client" in api_url:
            csv_path = DATA_DIR / "client_data.csv"
        else:
            csv_path = DATA_DIR / "internal_data.csv"

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path}")

        df = pd.read_csv(csv_path)
        duration = time.time() - start_time
        logger.info(f"Successfully fetched {len(df)} records from {api_url} in {duration:.2f}s")
        return df

    except Exception as e:
        logger.exception(f"Failed to fetch data from {api_url}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# Client Data
# ---------------------------------------------------------------------
def load_client_data(source: str = "api") -> pd.DataFrame:
    """
    Load client-side job data.
    source: "api" | "csv" | "sample"
    """
    logger.info(f"Loading client data (source='{source}')")

    try:
        if source == "api":
            df = fetch_from_api(CLIENT_API_URL)
        elif source == "csv":
            csv_path = DATA_DIR / "client_jobs.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Client CSV missing at {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from local CSV: {csv_path}")
        else:
            logger.warning("Falling back to sample client data (mock_client)")
            df = fetch_from_api("mock_client")

        logger.debug(f"Client DataFrame columns: {list(df.columns)}")
        return df

    except Exception as e:
        logger.exception(f"Error loading client data: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# Internal Ledger Data
# ---------------------------------------------------------------------
def load_internal_data(source: str = "api") -> pd.DataFrame:
    """
    Load internal ledger data.
    source: "api" | "csv" | "sample"
    """
    logger.info(f"Loading internal ledger data (source='{source}')")

    try:
        if source == "api":
            df = fetch_from_api(LEDGER_API_URL)
        elif source == "csv":
            csv_path = DATA_DIR / "internal_ledger.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Internal CSV missing at {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from local CSV: {csv_path}")
        else:
            logger.warning("Falling back to sample ledger data (mock_ledger)")
            df = fetch_from_api("mock_ledger")

        logger.debug(f"Ledger DataFrame columns: {list(df.columns)}")
        return df

    except Exception as e:
        logger.exception(f"Error loading ledger data: {e}")
        return pd.DataFrame()
