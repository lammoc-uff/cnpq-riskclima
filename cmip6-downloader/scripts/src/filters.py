# src/filters.py

import pandas as pd
from pathlib import Path

# Import filtering lists from the project configuration
from src.config import (
                        source_ids,
                        experiment_ids,
                        table_ids,
                        variable_ids,
                        group_fields
                        )


def normalize_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize selected CMIP6 fields by stripping whitespace and ensuring string type.

    Parameters:
        df (pd.DataFrame): Input CMIP6 catalog as a DataFrame.

    Returns:
        pd.DataFrame: DataFrame with normalized fields.
    """
    # Loop through the main CMIP6 identifiers and clean them
    for col in ["source_id", "experiment_id", 
                "member_id", "table_id", "variable_id", "grid_label", "version"]:
        df[col] = df[col].astype(str).str.strip()  # Ensure type str and remove extra spaces
    return df


def create_unique_key(df: pd.DataFrame) -> pd.Series:
    """
    Generate a unique string identifier for each dataset entry in the catalog.

    The key is built using a combination of CMIP6 fields.

    Parameters:
        df (pd.DataFrame): DataFrame with normalized CMIP6 metadata.

    Returns:
        pd.Series: Series of unique string keys.
    """
    return (
        df["source_id"] + "_" +
        df["experiment_id"] + "_" +
        df["member_id"] + "_" +
        df["table_id"] + "_" +
        df["variable_id"] + "_" +
        df["grid_label"] + "_" +
        df["version"]
    )


def create_group_key(df: pd.DataFrame) -> pd.Series:
    """
    Generate a group identifier (ignores member_id and version).

    Example:
        MIROC6_historical_day_tas_gn
    """
    parts = [df[field] for field in group_fields if field in df.columns]
    return pd.Series(["_".join(vals) for vals in zip(*parts)], index=df.index)


def group_relpath(record) -> Path:
    """
    Build the relative path for a group as:
        <source_id>/<experiment_id>/<table_id>/<variable_id>/<grid_label>

    Accepts a dict-like or pandas Series.
    """
    get = (record.get if hasattr(record, "get") else record.__getitem__)
    return (
        Path(str(get("source_id")))
        / str(get("experiment_id"))
        / str(get("table_id"))
        / str(get("variable_id"))
        / str(get("grid_label"))
    )


def filter_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply project-specific filters to the CMIP6 catalog.

    Filters by source_id, experiment_id, table_id, and variable_id using values
    configured in the project.

    Parameters:
        df (pd.DataFrame): Full CMIP6 catalog DataFrame.

    Returns:
        pd.DataFrame: Filtered catalog matching the defined criteria.
    """
    df_filtered = df[
        df["source_id"].isin(source_ids) &
        df["experiment_id"].isin(experiment_ids) &
        df["table_id"].isin(table_ids) &
        df["variable_id"].isin(variable_ids)
    ].copy()

    # Reset index after filtering
    df_filtered.reset_index(drop=True, inplace=True)
    return df_filtered