from __future__ import annotations

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BlockingSettings(BaseSettings):
    """Shared configuration for the atmospheric blocking workflows."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    base_directory: Path = Field(default=Path(), validation_alias="BLOCKING_BASE_DIRECTORY")
    persistence_days: int = Field(default=3, validation_alias="BLOCKING_PERSISTENCE_DAYS", gt=0)

    era5_climatology_start_year: int = Field(
        default=1991, validation_alias="ERA5_CLIMATOLOGY_START_YEAR"
    )
    era5_climatology_end_year: int = Field(
        default=2020, validation_alias="ERA5_CLIMATOLOGY_END_YEAR"
    )
    era5_climatology_label: str = Field(default="90_20", validation_alias="ERA5_CLIMATOLOGY_LABEL")
    era5_series_start_year: int = Field(default=1960, validation_alias="ERA5_SERIES_START_YEAR")
    era5_series_end_year: int = Field(default=2025, validation_alias="ERA5_SERIES_END_YEAR")
    era5_download_workers: int = Field(default=5, validation_alias="ERA5_DOWNLOAD_WORKERS", gt=0)
    era5_raw_directory: Path = Field(
        default=Path("era5/raw_data"), validation_alias="ERA5_RAW_DIRECTORY"
    )
    era5_processed_directory: Path = Field(
        default=Path("era5/processed_data"), validation_alias="ERA5_PROCESSED_DIRECTORY"
    )
    era5_results_directory: Path = Field(
        default=Path("era5/results"), validation_alias="ERA5_RESULTS_DIRECTORY"
    )

    cmip6_source_id: str = Field(default="BCC-CSM2-MR", validation_alias="CMIP6_SOURCE_ID")
    cmip6_climatology_experiment_id: str = Field(
        default="historical", validation_alias="CMIP6_CLIMATOLOGY_EXPERIMENT_ID"
    )
    cmip6_experiment_id: str = Field(default="ssp585", validation_alias="CMIP6_EXPERIMENT_ID")
    cmip6_climatology_start_year: int = Field(
        default=1980, validation_alias="CMIP6_CLIMATOLOGY_START_YEAR"
    )
    cmip6_climatology_end_year: int = Field(
        default=2010, validation_alias="CMIP6_CLIMATOLOGY_END_YEAR"
    )
    cmip6_series_start_year: int = Field(default=2015, validation_alias="CMIP6_SERIES_START_YEAR")
    cmip6_series_end_year: int = Field(default=2050, validation_alias="CMIP6_SERIES_END_YEAR")
    cmip6_climatology_label: str = Field(
        default="80_10", validation_alias="CMIP6_CLIMATOLOGY_LABEL"
    )
    cmip6_data_directory: Path = Field(
        default=Path("cmip6/raw_data"), validation_alias="CMIP6_DATA_DIRECTORY"
    )
    cmip6_processed_directory: Path = Field(
        default=Path("cmip6/processed_data"), validation_alias="CMIP6_PROCESSED_DIRECTORY"
    )
    cmip6_results_directory: Path = Field(
        default=Path("cmip6/results"), validation_alias="CMIP6_RESULTS_DIRECTORY"
    )

    cdsapi_url: str = Field(
        default="https://cds.climate.copernicus.eu/api", validation_alias="CDSAPI_URL"
    )
    cdsapi_key: str | None = Field(default=None, validation_alias="CDSAPI_KEY")
    cdsapi_config_file: Path = Field(
        default=Path("~/.cdsapirc"), validation_alias="CDSAPI_CONFIG_FILE"
    )

    @model_validator(mode="after")
    def validate_periods(self) -> BlockingSettings:
        if self.era5_climatology_start_year > self.era5_climatology_end_year:
            raise ValueError("ERA5 climatology start must not be after its end")
        if self.era5_series_start_year > self.era5_series_end_year:
            raise ValueError("ERA5 series start must not be after its end")
        if self.cmip6_climatology_start_year > self.cmip6_climatology_end_year:
            raise ValueError("CMIP6 climatology start must not be after its end")
        if self.cmip6_series_start_year > self.cmip6_series_end_year:
            raise ValueError("CMIP6 series start must not be after its end")
        return self

    def path(self, configured_path: Path) -> Path:
        """Resolve a configured path relative to the blocking project."""
        if configured_path.is_absolute():
            return configured_path
        return self.base_directory / configured_path

    def cds_key(self) -> str | None:
        """Return the configured CDS key or the key from the fallback file."""
        if self.cdsapi_key and self.cdsapi_key.strip():
            return self.cdsapi_key.strip()
        path = self.cdsapi_config_file.expanduser()
        if not path.is_file():
            return None
        for line in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition(":")
            if separator and key.strip() == "key" and value.strip():
                return value.strip()
        return None
