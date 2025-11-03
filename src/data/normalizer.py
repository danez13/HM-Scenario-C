import pandas as pd
def _normalize_text(value: str) -> str:
    """Normalize free-text fields (trim, lower, remove abbreviations)."""
    if not isinstance(value, str):
        return ""
    value = value.strip().lower()
    address_mapping = {
        "st.": "street",
        "rd.": "road",
        "ave": "avenue",
        "blvd": "boulevard",
    }
    for abbr, full in address_mapping.items():
        value = value.replace(abbr, full)
    return value.title()

def _normalize_service_type(value: str) -> str:
    """Ensure consistent service type naming."""
    if not isinstance(value, str):
        return ""
    return value.strip().upper()

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply normalization rules to the incoming dataset."""
    if df.empty:
        return df

    df = df.copy()
    df["job_date"] = pd.to_datetime(df["job_date"], errors="coerce")
    df["site"] = df["site"].apply(_normalize_text)
    df["service_type"] = df["service_type"].apply(_normalize_service_type)

    return df