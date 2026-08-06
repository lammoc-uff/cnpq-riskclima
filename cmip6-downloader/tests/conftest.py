"""Shared offline test fixtures."""

from pathlib import Path

import pandas as pd
import pytest

from src.config import Settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def settings() -> Settings:
    """Return settings loaded only from the checked-in example."""
    return Settings(_env_file=PROJECT_ROOT / ".env.example")


@pytest.fixture
def catalog_row() -> dict[str, str]:
    """Return one valid catalog row."""
    return {
        "source_id": "MIROC6",
        "experiment_id": "historical",
        "table_id": "day",
        "variable_id": "tas",
        "grid_label": "gn",
        "member_id": "r1i1p1f1",
        "version": "v20200101",
        "zstore": "s3://bucket/tas.zarr",
    }


@pytest.fixture
def catalog(catalog_row: dict[str, str]) -> pd.DataFrame:
    """Return a one-row valid catalog."""
    return pd.DataFrame([catalog_row])
