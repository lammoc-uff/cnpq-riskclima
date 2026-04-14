# src/compare.py

from pathlib import Path
import pandas as pd
from src.filters import normalize_fields, create_unique_key


def compare_cmip6_catalogs(
    aws_path: Path,
    google_path: Path,
    output_dir: Path,
    source_ids: list,
    experiment_ids: list,
    table_ids: list,
    variable_ids: list
) -> None:
    """
    Compare AWS and Google CMIP6 catalogs and save the differences and filtered catalogs.

    Parameters:
        aws_path (Path): Path to the AWS catalog CSV.
        google_path (Path): Path to the Google catalog CSV.
        output_dir (Path): Where to save the outputs.
        source_ids, experiment_ids, table_ids, variable_ids (list): Filters.
    """
    # Load catalogs
    df_aws = pd.read_csv(aws_path)
    df_google = pd.read_csv(google_path)

    # Filter activity and grid
    df_aws = df_aws.query('activity_id == "ScenarioMIP" or activity_id == "CMIP"')
    df_aws = df_aws.query('grid_label == "gn" or grid_label == "gr" or grid_label == "gr1"')
    df_google = df_google.query('activity_id == "ScenarioMIP" or activity_id == "CMIP"')
    df_google = df_google.query('grid_label == "gn" or grid_label == "gr" or grid_label == "gr1"')
    

    # Drop unnecessary columns
    drop_cols = ['dcpp_init_year', 'index', 'activity_id']
    df_aws.drop(columns=[c for c in drop_cols if c in df_aws.columns], inplace=True)
    df_google.drop(columns=[c for c in drop_cols if c in df_google.columns], inplace=True)

    # Sort catalog for consistency
    sort_cols = [
        "institution_id", "source_id", "experiment_id",
        "variable_id", "table_id", "member_id", "version"
    ]
    df_aws = df_aws.sort_values(by=sort_cols).reset_index(drop=True)
    df_google = df_google.sort_values(by=sort_cols).reset_index(drop=True)

    # Apply filters
    df_aws = df_aws[
        df_aws["source_id"].isin(source_ids) &
        df_aws["experiment_id"].isin(experiment_ids) &
        df_aws["table_id"].isin(table_ids) &
        df_aws["variable_id"].isin(variable_ids)
    ].copy()

    df_google = df_google[
        df_google["source_id"].isin(source_ids) &
        df_google["experiment_id"].isin(experiment_ids) &
        df_google["table_id"].isin(table_ids) &
        df_google["variable_id"].isin(variable_ids)
    ].copy()

    # Save filtered catalogs
    df_aws.to_csv(output_dir / "catalog_cnpq_aws.csv", index=False)
    df_google.to_csv(output_dir / "catalog_cnpq_google.csv", index=False)
    print("✅ Filtered catalogs saved: catalog_cnpq_aws.csv, catalog_cnpq_google.csv")

    # Normalize and generate keys
    df_aws = normalize_fields(df_aws)
    df_google = normalize_fields(df_google)

    df_aws["key"] = create_unique_key(df_aws)
    df_google["key"] = create_unique_key(df_google)

    df_aws.drop_duplicates(subset="key", inplace=True)
    df_google.drop_duplicates(subset="key", inplace=True)

    # Compare keys
    keys_aws = set(df_aws["key"])
    keys_google = set(df_google["key"])

    df_google_diff = df_google[~df_google["key"].isin(keys_aws)].copy()
    df_aws_diff = df_aws[~df_aws["key"].isin(keys_google)].copy()

    df_google_diff.drop(columns="key", inplace=True)
    df_aws_diff.drop(columns="key", inplace=True)

    # Save diffs
    df_google_diff.to_csv(output_dir / "catalog_diff_google_vs_aws.csv", index=False)
    print("✅ Diff Google - AWS saved.")

    df_aws_diff.to_csv(output_dir / "catalog_diff_aws_vs_google.csv", index=False)
    print("✅ Diff AWS - Google saved.")