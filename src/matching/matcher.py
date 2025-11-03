"""
matcher.py
-----------
Performs deterministic and fuzzy matching between client and internal job records.

- Exact match on shared keys (job_date, service_type)
- Fuzzy match on site names (free-text, abbreviations, misspellings)
- Returns confidence scores and match type
- Identifies unmatched records
"""

import pandas as pd
from difflib import SequenceMatcher
from typing import Tuple, List
import logging
import os

# matcher.py
from utils.logger_config import get_logger

logger = get_logger(__name__)



# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _similarity(a: str, b: str) -> float:
    """Return a similarity ratio between 0 and 1 for fuzzy string comparison."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, str(a).lower().strip(), str(b).lower().strip()).ratio()


def _normalize(s: str) -> str:
    """Normalize string for matching (lowercase, strip)."""
    return str(s).lower().strip() if isinstance(s, str) else ""


# ---------------------------------------------------------------------
# Matching Logic
# ---------------------------------------------------------------------
def get_unmatched(df: pd.DataFrame, matched_df: pd.DataFrame, join_keys: List[str]) -> pd.DataFrame:
    """Returns unmatched records from df based on matched_df and join_keys."""
    if matched_df.empty:
        logger.debug("No matched records found; all records are unmatched.")
        return df.copy()
    unmatched = df.merge(matched_df[join_keys], on=join_keys, how="left", indicator=True)
    unmatched = unmatched[unmatched["_merge"] == "left_only"].drop(columns=["_merge"])
    return unmatched


def deterministic_match(client_df: pd.DataFrame, internal_df: pd.DataFrame, join_keys: List[str]) -> pd.DataFrame:
    """Performs deterministic (exact key) matching on shared columns."""
    if not join_keys:
        raise ValueError(
            f"Matching failed: No common merge keys found.\n"
            f"Available client cols: {client_df.columns.tolist()}, "
            f"internal cols: {internal_df.columns.tolist()}"
        )

    logger.info(f"Performing deterministic match on keys: {join_keys}")
    matched = pd.merge(
        client_df,
        internal_df,
        on=join_keys,
        suffixes=("_client", "_internal"),
        how="inner"
    )
    matched["confidence"] = 1.0
    matched["match_type"] = "deterministic"

    logger.info(f"Deterministic matches found: {len(matched)}")
    return matched


def fuzzy_match(client_df: pd.DataFrame, internal_df: pd.DataFrame, threshold: float = 0.8) -> pd.DataFrame:
    """
    Perform fuzzy matching on site names when deterministic match fails.
    Returns best matches above threshold with confidence scores.
    """
    logger.info(f"Starting fuzzy matching (threshold={threshold})...")
    if client_df.empty or internal_df.empty:
        logger.warning("Fuzzy matching skipped: one or both DataFrames are empty.")
        return pd.DataFrame()

    results = []
    client_df = client_df.copy()
    internal_df = internal_df.copy()

    # Pre-group by date to avoid O(n²) comparisons across unrelated dates
    grouped_internal = internal_df.groupby("job_date")

    for _, c_row in client_df.iterrows():
        job_date = c_row.get("job_date")
        if pd.isna(job_date) or job_date not in grouped_internal.groups:
            continue

        best_score = 0.0
        best_match = None
        for _, i_row in grouped_internal.get_group(job_date).iterrows():
            name_sim = _similarity(c_row.get("site"), i_row.get("site"))
            type_sim = _similarity(c_row.get("service_type", ""), i_row.get("service_type", ""))
            score = (name_sim + type_sim) / 2

            if score > best_score:
                best_score = score
                best_match = i_row

        if best_match is not None and best_score >= threshold:
            combined = {**c_row.to_dict(), **best_match.to_dict()}
            combined["confidence"] = round(best_score, 2)
            combined["match_type"] = "fuzzy"
            results.append(combined)

    matched_count = len(results)
    logger.info(f"Fuzzy matches found: {matched_count}")
    return pd.DataFrame(results)


def reconcile(client_df: pd.DataFrame, internal_df: pd.DataFrame, threshold: float = 0.8) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform deterministic + fuzzy matching.
    Returns:
        all_matches, unmatched_client, unmatched_internal
    """
    if client_df.empty or internal_df.empty:
        raise ValueError("One or both input DataFrames are empty.")

    logger.info(f"Reconciling datasets: client({len(client_df)}) vs internal({len(internal_df)})")

    client_df = client_df.copy()
    internal_df = internal_df.copy()
    client_df["site"] = client_df["site"].apply(_normalize)
    internal_df["site"] = internal_df["site"].apply(_normalize)

    possible_keys = ["job_date", "site", "service_type"]
    join_keys = [k for k in possible_keys if k in client_df.columns and k in internal_df.columns]
    if not join_keys:
        raise ValueError("No overlapping columns for matching found between datasets.")

    # Step 1: Deterministic match
    det_matched = deterministic_match(client_df, internal_df, join_keys)

    # Step 2: Fuzzy match on unmatched
    unmatched_client = get_unmatched(client_df, det_matched, join_keys)
    unmatched_internal = get_unmatched(internal_df, det_matched, join_keys)
    fuzzy_matched = fuzzy_match(unmatched_client, unmatched_internal, threshold)

    # Step 3: Combine all matches
    all_matches = pd.concat([det_matched, fuzzy_matched], ignore_index=True)

    # Step 4: Identify remaining unmatched
    matched_client_ids = all_matches.get("order_id", pd.Series(dtype=object)).dropna().unique().tolist()
    matched_internal_ids = all_matches.get("job_id", pd.Series(dtype=object)).dropna().unique().tolist()

    client_unmatched = client_df[~client_df.get("order_id", pd.Series(dtype=object)).isin(matched_client_ids)].reset_index(drop=True)
    internal_unmatched = internal_df[~internal_df.get("job_id", pd.Series(dtype=object)).isin(matched_internal_ids)].reset_index(drop=True)

    logger.info(f"Total matches: {len(all_matches)} | Unmatched client: {len(client_unmatched)} | Unmatched internal: {len(internal_unmatched)}")

    return all_matches, client_unmatched, internal_unmatched
