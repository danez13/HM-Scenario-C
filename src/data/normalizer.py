"""
normalizer.py
--------------
Standardizes and cleans incoming dataframes from client and internal sources.

Responsibilities:
 - Normalize free-text fields (e.g., site names)
 - Standardize service type formatting
 - Ensure valid job dates
 - Log normalization steps and data quality issues
"""

import pandas as pd

# matcher.py
from utils.logger_config import get_logger

logger = get_logger(__name__)



# ---------------------------------------------------------------------
# Normalization Helpers
# ---------------------------------------------------------------------
def _normalize_text(value: str) -> str:
    """Normalize free-text fields (trim, lowercase, replace abbreviations)."""
    if not isinstance(value, str):
        return ""
    original = value
    value = value.strip().lower()

    address_mapping = {
        "st.": "street",
        "rd.": "road",
        "ave": "avenue",
        "blvd": "boulevard",
        "hwy": "highway",
        "plz": "plaza"
    }

    for abbr, full in address_mapping.items():
        value = value.replace(abbr, full)

    normalized = value.title()

    if original != normalized:
        logger.debug(f"Normalized site text: '{original}' → '{normalized}'")

    return normalized


def _normalize_service_type(value: str) -> str:
    """Ensure consistent service type naming."""
    if not isinstance(value, str):
        return ""
    original = value
    normalized = value.strip().upper()

    if original != normalized:
        logger.debug(f"Normalized service type: '{original}' → '{normalized}'")

    return normalized


# ---------------------------------------------------------------------
# DataFrame Normalization
# ---------------------------------------------------------------------
def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply normalization rules to the incoming dataset."""
    if df.empty:
        logger.warning("Received empty DataFrame for normalization.")
        return df

    logger.info(f"Starting normalization for {len(df)} rows...")

    df = df.copy()

    # Validate required columns
    required_cols = ["job_date", "site", "service_type"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise KeyError(f"Missing columns: {missing_cols}")

    # Normalize job_date
    df["job_date"] = pd.to_datetime(df["job_date"], errors="coerce")
    invalid_dates = df["job_date"].isna().sum()
    if invalid_dates > 0:
        logger.warning(f"{invalid_dates} invalid or missing 'job_date' entries coerced to NaT")

    # Normalize text columns
    df["site"] = df["site"].apply(_normalize_text)
    df["service_type"] = df["service_type"].apply(_normalize_service_type)

    logger.info(
        f"Normalization complete — {len(df)} records processed, {invalid_dates} invalid dates found."
    )
    return df
