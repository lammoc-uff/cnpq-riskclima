from datetime import date
from pathlib import Path
from string import Formatter
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

type NetCDFEngine = Literal["netcdf4", "scipy"]
type NetCDFFormat = Literal["NETCDF4", "NETCDF4_CLASSIC", "NETCDF3_64BIT", "NETCDF3_CLASSIC"]

CMIP6_OUTPUT_PLACEHOLDERS = frozenset(
    {"scale_months", "model", "experiment", "member", "grid", "start", "end"}
)


class SPISettings(BaseSettings):
    """Shared operational configuration for an SPI source."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    spi_scale_months: int = Field(validation_alias="SPI_SCALE_MONTHS", gt=0)
    spi_distribution: str = Field(validation_alias="SPI_DISTRIBUTION", min_length=1)
    spi_method: str = Field(validation_alias="SPI_METHOD", min_length=1)
    spi_floc: float = Field(validation_alias="SPI_FLOC")
    spi_calibration_start: date
    spi_calibration_end: date
    spi_application_start: date
    spi_application_end: date
    spi_output_directory: Path
    spi_output_template: str = Field(min_length=1)

    netcdf_engine: NetCDFEngine = Field(validation_alias="NETCDF_ENGINE")
    netcdf_format: NetCDFFormat = Field(validation_alias="NETCDF_FORMAT")
    netcdf_compression: bool = Field(validation_alias="NETCDF_COMPRESSION")
    netcdf_complevel: int = Field(validation_alias="NETCDF_COMPLEVEL", ge=0, le=9)

    metadata_creators: str = Field(validation_alias="METADATA_CREATORS")
    metadata_institution: str = Field(validation_alias="METADATA_INSTITUTION")
    metadata_project: str = Field(validation_alias="METADATA_PROJECT")
    metadata_license: str = Field(validation_alias="METADATA_LICENSE")
    metadata_references: str = Field(validation_alias="METADATA_REFERENCES")
    metadata_repository: str = Field(validation_alias="METADATA_REPOSITORY")
    metadata_conventions: str = Field(validation_alias="METADATA_CONVENTIONS")
    metadata_processing_level: str = Field(validation_alias="METADATA_PROCESSING_LEVEL")

    log_level: str = Field(validation_alias="LOG_LEVEL")
    log_format: str = Field(validation_alias="LOG_FORMAT")

    @model_validator(mode="after")
    def validate_shared_configuration(self) -> "SPISettings":
        """Validate periods and the selected NetCDF implementation."""
        if self.spi_calibration_start > self.spi_calibration_end:
            raise ValueError("SPI calibration start must not be after its end")
        if self.spi_application_start > self.spi_application_end:
            raise ValueError("SPI application start must not be after its end")
        if self.netcdf_engine == "netcdf4" and self.netcdf_format not in {
            "NETCDF4",
            "NETCDF4_CLASSIC",
        }:
            raise ValueError("netcdf4 requires NETCDF4 or NETCDF4_CLASSIC")
        if self.netcdf_engine == "scipy" and self.netcdf_format not in {
            "NETCDF3_64BIT",
            "NETCDF3_CLASSIC",
        }:
            raise ValueError("scipy requires a NETCDF3 format")
        if self.netcdf_engine == "scipy" and self.netcdf_compression:
            raise ValueError("scipy does not support NetCDF compression")
        return self


class CMIP6Settings(SPISettings):
    """Operational configuration for one CMIP6 SPI execution."""

    cmip6_input_file: Path = Field(validation_alias="CMIP6_INPUT_FILE")
    cmip6_calibration_input_file: Path = Field(validation_alias="CMIP6_CALIBRATION_INPUT_FILE")
    cmip6_model: str = Field(validation_alias="CMIP6_MODEL", min_length=1)
    cmip6_experiment: str = Field(validation_alias="CMIP6_EXPERIMENT", min_length=1)
    cmip6_member: str = Field(validation_alias="CMIP6_MEMBER", min_length=1)
    cmip6_grid: str = Field(validation_alias="CMIP6_GRID", min_length=1)
    cmip6_precipitation_variable: str = Field(validation_alias="CMIP6_PRECIPITATION_VARIABLE")
    cmip6_time_dimension: str = Field(validation_alias="CMIP6_TIME_DIMENSION")
    cmip6_latitude_dimension: str = Field(validation_alias="CMIP6_LATITUDE_DIMENSION")
    cmip6_longitude_dimension: str = Field(validation_alias="CMIP6_LONGITUDE_DIMENSION")

    spi_calibration_start: date = Field(validation_alias="CMIP6_CALIBRATION_START")
    spi_calibration_end: date = Field(validation_alias="CMIP6_CALIBRATION_END")
    spi_application_start: date = Field(validation_alias="CMIP6_APPLICATION_START")
    spi_application_end: date = Field(validation_alias="CMIP6_APPLICATION_END")
    spi_output_directory: Path = Field(validation_alias="CMIP6_OUTPUT_DIRECTORY")
    spi_output_template: str = Field(validation_alias="CMIP6_OUTPUT_TEMPLATE", min_length=1)

    @field_validator(
        "cmip6_experiment",
        "cmip6_model",
        "cmip6_member",
        "cmip6_grid",
        "cmip6_precipitation_variable",
        "cmip6_time_dimension",
        "cmip6_latitude_dimension",
        "cmip6_longitude_dimension",
    )
    @classmethod
    def reject_blank_values(cls, value: str) -> str:
        """Reject blank CMIP6 identifiers."""
        if not value.strip():
            raise ValueError("CMIP6 configuration values must not be blank")
        return value

    @field_validator("spi_output_template")
    @classmethod
    def validate_output_template(cls, value: str) -> str:
        """Require all and only the supported CMIP6 identity placeholders."""
        try:
            placeholders = {
                field_name
                for _, field_name, _, _ in Formatter().parse(value)
                if field_name is not None
            }
        except ValueError as error:
            raise ValueError(f"Invalid CMIP6 output template: {error}") from error

        missing = CMIP6_OUTPUT_PLACEHOLDERS - placeholders
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"CMIP6 output template is missing required placeholders: {names}")

        unknown = placeholders - CMIP6_OUTPUT_PLACEHOLDERS
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"CMIP6 output template contains unknown placeholders: {names}")

        return value

    def output_path(self) -> Path:
        """Return the CMIP6 SPI output path."""
        filename = self.spi_output_template.format(
            scale_months=self.spi_scale_months,
            model=self.cmip6_model,
            experiment=self.cmip6_experiment,
            member=self.cmip6_member,
            grid=self.cmip6_grid,
            start=self.spi_application_start.isoformat(),
            end=self.spi_application_end.isoformat(),
        )
        return self.spi_output_directory / filename


class ERA5Settings(SPISettings):
    """Operational configuration for ERA5 acquisition and SPI calculation."""

    era5_dataset: str = Field(validation_alias="ERA5_DATASET", min_length=1)
    era5_product_type: str = Field(validation_alias="ERA5_PRODUCT_TYPE", min_length=1)
    era5_request_variable: str = Field(validation_alias="ERA5_REQUEST_VARIABLE", min_length=1)
    era5_download_start: date = Field(validation_alias="ERA5_DOWNLOAD_START")
    era5_download_end: date = Field(validation_alias="ERA5_DOWNLOAD_END")
    era5_time: str = Field(validation_alias="ERA5_TIME", min_length=1)
    era5_data_format: str = Field(validation_alias="ERA5_DATA_FORMAT", min_length=1)
    era5_download_format: str = Field(validation_alias="ERA5_DOWNLOAD_FORMAT", min_length=1)
    era5_latitude_min: float = Field(validation_alias="ERA5_LATITUDE_MIN", ge=-90, le=90)
    era5_latitude_max: float = Field(validation_alias="ERA5_LATITUDE_MAX", ge=-90, le=90)
    era5_longitude_min: float = Field(validation_alias="ERA5_LONGITUDE_MIN", ge=-180, le=180)
    era5_longitude_max: float = Field(validation_alias="ERA5_LONGITUDE_MAX", ge=-180, le=180)
    era5_raw_file_template: str = Field(validation_alias="ERA5_RAW_FILE_TEMPLATE", min_length=1)
    era5_spatial_chunk: int = Field(validation_alias="ERA5_SPATIAL_CHUNK", gt=0)
    era5_dask_workers: int = Field(validation_alias="ERA5_DASK_WORKERS", gt=0)
    era5_precipitation_variable: str = Field(validation_alias="ERA5_PRECIPITATION_VARIABLE")
    era5_time_dimension: str = Field(validation_alias="ERA5_TIME_DIMENSION")
    era5_latitude_dimension: str = Field(validation_alias="ERA5_LATITUDE_DIMENSION")
    era5_longitude_dimension: str = Field(validation_alias="ERA5_LONGITUDE_DIMENSION")

    spi_calibration_start: date = Field(validation_alias="ERA5_CALIBRATION_START")
    spi_calibration_end: date = Field(validation_alias="ERA5_CALIBRATION_END")
    spi_application_start: date = Field(validation_alias="ERA5_APPLICATION_START")
    spi_application_end: date = Field(validation_alias="ERA5_APPLICATION_END")
    spi_output_directory: Path = Field(validation_alias="ERA5_OUTPUT_DIRECTORY")
    spi_output_template: str = Field(validation_alias="ERA5_OUTPUT_TEMPLATE", min_length=1)

    @model_validator(mode="after")
    def validate_era5_configuration(self) -> "ERA5Settings":
        """Validate the ERA5 download period and geographic bounds."""
        if self.era5_download_start > self.era5_download_end:
            raise ValueError("ERA5 download start must not be after its end")
        if self.era5_download_start.month != 1 or self.era5_download_start.day != 1:
            raise ValueError("ERA5 download start must be January 1")
        if self.era5_download_end.day != 1:
            raise ValueError("ERA5 download end must be the first day of a month")
        if self.era5_latitude_min >= self.era5_latitude_max:
            raise ValueError("ERA5 latitude minimum must be below its maximum")
        if self.era5_longitude_min >= self.era5_longitude_max:
            raise ValueError("ERA5 longitude minimum must be below its maximum")
        if not (
            self.era5_download_start
            <= self.spi_calibration_start
            <= self.spi_calibration_end
            <= self.era5_download_end
        ):
            raise ValueError("ERA5 download must cover the complete calibration period")
        if not (
            self.era5_download_start
            <= self.spi_application_start
            <= self.spi_application_end
            <= self.era5_download_end
        ):
            raise ValueError("ERA5 download must cover the complete application period")
        return self

    def raw_input_path(self) -> Path:
        """Return the requested final ERA5 input path."""
        return Path(
            self.era5_raw_file_template.format(
                start=self.era5_download_start.isoformat(),
                end=self.era5_download_end.isoformat(),
            )
        )

    def output_path(self) -> Path:
        """Return the ERA5 SPI output path."""
        filename = self.spi_output_template.format(
            scale_months=self.spi_scale_months,
            start=self.spi_application_start.isoformat(),
            end=self.spi_application_end.isoformat(),
        )
        return self.spi_output_directory / filename


class CDSCredentials(BaseSettings):
    """Credentials used only when an ERA5 download is required."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    cdsapi_url: str = Field(validation_alias="CDSAPI_URL", min_length=1)
    cdsapi_key: SecretStr | None = Field(default=None, validation_alias="CDSAPI_KEY")
    cdsapi_config_file: Path = Field(validation_alias="CDSAPI_CONFIG_FILE")
