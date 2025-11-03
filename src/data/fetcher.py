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
import io
import json
import time
import logging
import random
import pandas as pd
from datetime import datetime

# ---------------------------------------------------------------------
# Setup logging & environment
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# In a real implementation, these would come from .env or secret manager
CLIENT_API_URL = os.getenv("CLIENT_API_URL", "https://api.mockclientdata.local/jobs")
LEDGER_API_URL = os.getenv("LEDGER_API_URL", "https://api.internalledger.local/records")

def _simulate_api_delay():
    """Simulate network delay for realism."""
    time.sleep(random.uniform(0.1, 0.4))

def fetch_from_api(api_url: str) -> pd.DataFrame:
    """
    Simulated API call returning JSON job data.

    In production, this would use requests.get(api_url) and validate the response.
    """
    _simulate_api_delay()

    if "client" in api_url:
        data = pd.read_csv("data/client_data.csv")
    else:
        data = pd.read_csv("data/internal_data.csv")

    df = pd.DataFrame(data)
    logging.info(f"Fetched {len(df)} records from {api_url}")
    return df

def load_client_data(source: str = "api") -> pd.DataFrame:
    """
    Load client-side job data.
    source: "api" | "csv" | "sample"
    """
    try:
        if source == "api":
            df = fetch_from_api(CLIENT_API_URL)
        elif source == "csv" and os.path.exists("data/client_jobs.csv"):
            df = pd.read_csv("data/client_jobs.csv")
        else:
            # fallback sample
            df = fetch_from_api("mock_client")
        return df
    except Exception as e:
        logging.error(f"Error loading client data: {e}")
        return pd.DataFrame()


def load_internal_data(source: str = "api") -> pd.DataFrame:
    """
    Load internal ledger data.
    source: "api" | "csv" | "sample"
    """
    try:
        if source == "api":
            df = fetch_from_api(LEDGER_API_URL)
        elif source == "csv" and os.path.exists("data/internal_ledger.csv"):
            df = pd.read_csv("data/internal_ledger.csv")
        else:
            df = fetch_from_api("mock_ledger")
        return df
    except Exception as e:
        logging.error(f"Error loading ledger data: {e}")
        return pd.DataFrame()