from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SACZSettings(BaseSettings):
    """Operational configuration shared by the SACZ workflows."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    base_directory: Path = Field(default=Path(), validation_alias="SACZ_BASE_DIRECTORY")
    areas_file: Path = Field(
        default=Path("areas/sams_index_calc_areas.shp"),
        validation_alias="SACZ_AREAS_FILE",
    )
    coefficients_directory: Path = Field(
        default=Path("coefs"), validation_alias="SACZ_COEFFICIENTS_DIRECTORY"
    )

    era5_year: int | None = Field(default=None, validation_alias="ERA5_YEAR")
    era5_raw_directory: Path = Field(
        default=Path("era5/raw_data"), validation_alias="ERA5_RAW_DIRECTORY"
    )
    era5_input_directory: Path = Field(
        default=Path("era5/processed_data"), validation_alias="ERA5_INPUT_DIRECTORY"
    )
    era5_intermediates_directory: Path = Field(
        default=Path("era5/processed_data/intermediates"),
        validation_alias="ERA5_INTERMEDIATES_DIRECTORY",
    )
    era5_output_directory: Path = Field(
        default=Path("era5/results"), validation_alias="ERA5_OUTPUT_DIRECTORY"
    )

    cmip6_data_directory: Path = Field(
        default=Path("cmip6/raw_data"), validation_alias="CMIP6_DATA_DIRECTORY"
    )
    cmip6_input_directory: Path = Field(
        default=Path("cmip6/processed_data"), validation_alias="CMIP6_INPUT_DIRECTORY"
    )
    cmip6_intermediates_directory: Path = Field(
        default=Path("cmip6/processed_data/intermediates"),
        validation_alias="CMIP6_INTERMEDIATES_DIRECTORY",
    )
    cmip6_output_directory: Path = Field(
        default=Path("cmip6/results"), validation_alias="CMIP6_OUTPUT_DIRECTORY"
    )
    cmip6_source_id: str = Field(default="BCC-CSM2-MR", validation_alias="CMIP6_SOURCE_ID")
    cmip6_experiment_id: str = Field(default="ssp245", validation_alias="CMIP6_EXPERIMENT_ID")
    cmip6_start_year: int = Field(default=2015, validation_alias="CMIP6_START_YEAR")
    cmip6_end_year: int = Field(default=2050, validation_alias="CMIP6_END_YEAR")

    def path(self, configured_path: Path) -> Path:
        """Resolve a configured path relative to the SACZ project directory."""
        if configured_path.is_absolute():
            return configured_path
        return self.base_directory / configured_path


def parse_settings(arguments: Sequence[str] | None = None) -> SACZSettings:
    """Load `.env` settings and override them with optional CLI arguments."""
    parser = argparse.ArgumentParser(description="Run the RiskClima SACZ workflow.")
    for name, option, value_type in (
        ("base_directory", "--base-directory", Path),
        ("areas_file", "--areas-file", Path),
        ("coefficients_directory", "--coefficients-directory", Path),
        ("era5_year", "--era5-year", int),
        ("era5_raw_directory", "--era5-raw-directory", Path),
        ("era5_input_directory", "--era5-input-directory", Path),
        ("era5_intermediates_directory", "--era5-intermediates-directory", Path),
        ("era5_output_directory", "--era5-output-directory", Path),
        ("cmip6_data_directory", "--cmip6-data-directory", Path),
        ("cmip6_input_directory", "--cmip6-input-directory", Path),
        ("cmip6_intermediates_directory", "--cmip6-intermediates-directory", Path),
        ("cmip6_output_directory", "--cmip6-output-directory", Path),
        ("cmip6_source_id", "--cmip6-source-id", str),
        ("cmip6_experiment_id", "--cmip6-experiment-id", str),
        ("cmip6_start_year", "--cmip6-start-year", int),
        ("cmip6_end_year", "--cmip6-end-year", int),
    ):
        parser.add_argument(option, dest=name, type=value_type, default=argparse.SUPPRESS)
    overrides = vars(parser.parse_args(arguments))
    return SACZSettings(**overrides)
