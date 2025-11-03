"""
matcher.py
-----------
Performs deterministic and fuzzy matching between client and internal job records.

- Uses exact match on shared columns (job_date, site, service_type)
- Applies fuzzy matching for site name differences
- Produces confidence scores
- Identifies unmatched or anomalous records
"""

from difflib import SequenceMatcher
import pandas as pd
from typing import Tuple

def _similarity(a: str, b: str) -> float:
    """Return a similarity ratio between 0 and 1 for fuzzy string comparison."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, str(a).lower().strip(), str(b).lower().strip()).ratio()

def get_umatched(df: pd.DataFrame, matched_df: pd.DataFrame, join_keys: list) -> pd.DataFrame:
    """Returns unmatched records from client_df based on matched_df and join_keys."""
    unmatched = df.merge(matched_df[join_keys], on=join_keys, how="left", indicator=True)
    unmatched = unmatched[unmatched["_merge"] == "left_only"].drop(columns=["_merge"])
    return unmatched

def deterministic_match(client_df: pd.DataFrame, internal_df: pd.DataFrame, join_keys:list) -> pd.DataFrame:
    """Performs deterministic (exact key) matching on shared columns."""

    if not join_keys:
        raise ValueError(
            f"Matching failed: No common merge keys found.\n"
            f"Available client cols: {client_df.columns.tolist()}, "
            f"internal cols: {internal_df.columns.tolist()}"
        )

    matched = pd.merge(
        client_df,
        internal_df,
        on=join_keys,
        suffixes=("_client", "_internal"),
        how="inner"
    )

    matched["confidence"] = 1.0

    return matched

def fuzzy_match(client_df: pd.DataFrame, internal_df: pd.DataFrame, threshold: float = 0.80) -> pd.DataFrame:
    """
    Perform fuzzy matching on site names and service types when deterministic match fails.
    Returns a DataFrame of best matches with confidence scores.
    """
    results = []

    for client_row in client_df.iloc():
        best_score = 0.0
        best_match = None

        for internal_row in internal_df.iloc():
            # Match only same date to constrain comparison
            if client_row.get("job_date") != internal_row.get("job_date"):
                continue

            name_sim = _similarity(client_row.get("site", ""), internal_row.get("site", ""))
            type_sim = _similarity(client_row.get("service_type", ""), internal_row.get("service_type", ""))
            score = (name_sim + type_sim) / 2

            if score > best_score:
                best_score = score
                best_match = internal_row

        if best_match is not None and best_score >= threshold:
            # **{f"{k}_internal": v for k, v in best_match.to_dict().items()}
            combined = {**client_row.to_dict(), **best_match.to_dict()}
            combined["confidence"] = round(best_score, 2)
            results.append(combined)

    return pd.DataFrame(results)

def reconcile(
    client_df: pd.DataFrame, internal_df: pd.DataFrame, threshold: float = 0.80
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform both deterministic and fuzzy matching, returning:
      - all matches
      - unmatched client records
      - unmatched internal records
    """
    if client_df.empty or internal_df.empty:
        raise ValueError("One or both input DataFrames are empty.")

    possible_keys = ["job_date", "site", "service_type"]
    join_keys = [key for key in possible_keys if key in client_df.columns and key in internal_df.columns]

    det_matched = deterministic_match(client_df, internal_df, join_keys)
    
    unmatched_client = get_umatched(client_df, det_matched, join_keys)
    unmatched_internal = get_umatched(internal_df, det_matched, join_keys)

    fuzzy_matched = fuzzy_match(unmatched_client, unmatched_internal, threshold=threshold)
    
    all_matches = pd.concat([det_matched, fuzzy_matched], ignore_index=True)

    matched_client_ids = all_matches['order_id'].dropna().tolist()
    matched_internal_ids = all_matches['job_id'].dropna().tolist()

    client_unmatched = client_df[~client_df['order_id'].isin(matched_client_ids)].reset_index(drop=True)
    internal_unmatched = internal_df[~internal_df['job_id'].isin(matched_internal_ids)].reset_index(drop=True)


    return all_matches, client_unmatched, internal_unmatched