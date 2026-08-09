"""CMIP6 catalog schema, filtering, and grouping helpers."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from src.config import GROUP_FIELDS, Settings

IDENTITY_COLUMNS = [
    "source_id",
    "experiment_id",
    "table_id",
    "variable_id",
    "grid_label",
    "member_id",
]
REQUIRED_CATALOG_COLUMNS = [*IDENTITY_COLUMNS, "version", "zstore"]
NORMALIZED_COLUMNS = [*IDENTITY_COLUMNS, "version"]
OPTIONAL_NORMALIZED_COLUMNS = [
    "provider",
    "alternate_provider",
    "zstore",
    "alternate_zstore",
]
COVERAGE_COLUMN_PAIRS = (
    ("time_start", "time_end"),
    ("start_date", "end_date"),
    ("temporal_start", "temporal_end"),
)
SUPPORTED_PROVIDER_SCHEMES = {"aws": "s3", "google": "gs"}
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")


def validate_catalog_schema(frame: pd.DataFrame) -> None:
    """Validate columns and values required by the operational pipeline."""
    missing = sorted(set(REQUIRED_CATALOG_COLUMNS).difference(frame.columns))
    if missing:
        raise ValueError(f"catalog is missing required columns: {', '.join(missing)}")
    for column in REQUIRED_CATALOG_COLUMNS:
        values = frame[column]
        if values.isna().any() or values.astype("string").str.strip().eq("").any():
            raise ValueError(f"catalog column {column} cannot contain null or blank values")
    for column in [*IDENTITY_COLUMNS, "version"]:
        invalid = ~frame[column].astype("string").str.strip().str.fullmatch(SAFE_IDENTIFIER)
        if invalid.any():
            raise ValueError(f"catalog column {column} contains an unsafe identifier")
    _validate_coverage(frame)
    alternate_columns = {"alternate_provider", "alternate_zstore"}
    present_alternate_columns = alternate_columns.intersection(frame.columns)
    if present_alternate_columns and present_alternate_columns != alternate_columns:
        raise ValueError("catalog has incomplete alternate provider columns")
    for row_number, (_, row) in enumerate(frame.iterrows()):
        provider = str(row["provider"]).strip() if "provider" in frame.columns else None
        if provider is not None and provider not in SUPPORTED_PROVIDER_SCHEMES:
            raise ValueError(f"catalog row {row_number} has unsupported provider: {provider}")
        _validate_zstore(str(row["zstore"]).strip(), provider, row_number)
        alternate = row.get("alternate_provider")
        alternate_zstore = row.get("alternate_zstore")
        alternate_provider = "" if pd.isna(alternate) else str(alternate).strip()
        alternate_store = "" if pd.isna(alternate_zstore) else str(alternate_zstore).strip()
        if not alternate_provider and not alternate_store:
            continue
        if not alternate_provider or not alternate_store:
            raise ValueError(f"catalog row {row_number} has incomplete alternate provider metadata")
        if alternate_provider not in SUPPORTED_PROVIDER_SCHEMES:
            raise ValueError(
                f"catalog row {row_number} has unsupported alternate provider: {alternate_provider}"
            )
        if provider is not None and alternate_provider == provider:
            raise ValueError(f"catalog row {row_number} alternate provider must differ")
        _validate_zstore(alternate_store, alternate_provider, row_number)


def _validate_coverage(frame: pd.DataFrame) -> None:
    present_pairs: list[tuple[str, str]] = []
    columns = set(frame.columns)
    for pair in COVERAGE_COLUMN_PAIRS:
        present = columns.intersection(pair)
        if present and len(present) != 2:
            raise ValueError(f"catalog has incomplete coverage pair: {pair[0]}, {pair[1]}")
        if len(present) == 2:
            present_pairs.append(pair)
    if len(present_pairs) > 1:
        raise ValueError("catalog has ambiguous temporal coverage column pairs")
    if not present_pairs:
        return

    start_column, end_column = present_pairs[0]
    for column in (start_column, end_column):
        values = frame[column]
        if values.isna().any() or values.astype("string").str.strip().eq("").any():
            raise ValueError(
                f"catalog coverage column {column} cannot contain null or blank values"
            )
    starts = pd.to_datetime(frame[start_column], errors="coerce", format="mixed")
    ends = pd.to_datetime(frame[end_column], errors="coerce", format="mixed")
    if starts.isna().any() or ends.isna().any():
        raise ValueError("catalog coverage values must be parseable dates")
    if (starts > ends).any():
        raise ValueError("catalog coverage start must not be after coverage end")


def _validate_zstore(zstore: str, provider: str | None, row_index: int) -> None:
    parsed = urlparse(zstore)
    if parsed.scheme not in set(SUPPORTED_PROVIDER_SCHEMES.values()) or not parsed.netloc:
        raise ValueError(f"catalog row {row_index} has invalid zstore: {zstore}")
    if provider is not None and SUPPORTED_PROVIDER_SCHEMES.get(provider) != parsed.scheme:
        raise ValueError(f"catalog row {row_index} zstore does not match provider {provider}")


def normalize_fields(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with normalized CMIP6 identifier fields."""
    normalized = frame.copy()
    for column in NORMALIZED_COLUMNS:
        normalized[column] = normalized[column].astype("string").str.strip()
    for column in OPTIONAL_NORMALIZED_COLUMNS:
        if column in normalized:
            normalized[column] = normalized[column].astype("string").str.strip()
    for column in ("alternate_provider", "alternate_zstore"):
        if column in normalized:
            normalized[column] = normalized[column].fillna("")
    validate_catalog_schema(normalized)
    return normalized


def filter_catalog(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Apply configured metadata filters to a validated catalog."""
    validate_catalog_schema(frame)
    mask = (
        frame["source_id"].isin(settings.source_ids)
        & frame["experiment_id"].isin(settings.experiment_ids)
        & frame["table_id"].isin(settings.table_ids)
        & frame["variable_id"].isin(settings.variable_ids)
        & frame["grid_label"].isin(settings.grid_labels)
    )
    if settings.member_ids:
        mask &= frame["member_id"].isin(settings.member_ids)
    return frame.loc[mask].reset_index(drop=True)


def create_unique_key(frame: pd.DataFrame) -> pd.Series:
    """Create a stable key including version and remote store."""
    if frame.empty:
        return pd.Series(index=frame.index, dtype="string")
    return frame[[*IDENTITY_COLUMNS, "version", "zstore"]].astype(str).agg("|".join, axis=1)


def create_group_key(frame: pd.DataFrame) -> pd.Series:
    """Create a key for output groups, excluding member and version."""
    if frame.empty:
        return pd.Series(index=frame.index, dtype="string")
    return frame[list(GROUP_FIELDS)].astype(str).agg("|".join, axis=1)


def group_relpath(record: pd.Series) -> Path:
    """Build a group's relative output path."""
    return Path(*(str(record[field]) for field in GROUP_FIELDS))


def version_key(value: str) -> tuple[int, str]:
    """Return a deterministic semantic ordering key for CMIP6 versions."""
    digits = "".join(character for character in value if character.isdigit())
    return (int(digits) if digits else -1, value)


def coverage_columns(frame: pd.DataFrame) -> tuple[str, str] | None:
    """Return the temporal coverage pair from a validated catalog."""
    _validate_coverage(frame)
    return next((pair for pair in COVERAGE_COLUMN_PAIRS if set(pair) <= set(frame.columns)), None)
