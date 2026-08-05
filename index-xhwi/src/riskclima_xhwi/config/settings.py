from enum import StrEnum
from pathlib import Path
from string import Formatter
from typing import Literal, override

import numpy as np
import torch
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

type CMIP6Scenario = str
type NetCDFEngine = Literal["netcdf4", "scipy"]
type NetCDFFormat = Literal["NETCDF4", "NETCDF4_CLASSIC", "NETCDF3_64BIT", "NETCDF3_CLASSIC"]

TEMPLATE_FIELDS = {
    "start_year",
    "end_year",
    "model",
    "member",
    "grid",
    "scenario",
    "month",
    "source",
}


class CalibrationPolicy(StrEnum):
    """Control how a calibration is obtained."""

    REQUIRE_EXISTING = "require_existing"
    CREATE_IF_MISSING = "create_if_missing"
    REBUILD = "rebuild"
    IN_MEMORY = "in_memory"


class ExistingFilePolicy(StrEnum):
    """Control how an existing output file is handled."""

    SKIP = "skip"
    OVERWRITE = "overwrite"
    FAIL = "fail"


class ConcatInputPolicy(StrEnum):
    """Select the monthly parts included in final concatenation."""

    ALL_MATCHING_PARTS = "all_matching_parts"
    CURRENT_RUN = "current_run"


class Device(StrEnum):
    """Supported PyTorch execution devices."""

    AUTO = "auto"
    CPU = "cpu"
    CUDA = "cuda"


class FloatingDtype(StrEnum):
    """Supported in-memory floating-point types."""

    FLOAT32 = "float32"
    FLOAT64 = "float64"


class LogLevel(StrEnum):
    """Supported textual logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class InterpolationMethod(StrEnum):
    """Supported xarray interpolation methods."""

    LINEAR = "linear"
    NEAREST = "nearest"


class CDSCredentials(BaseSettings):
    """Credentials used to authenticate requests to CDS ARCO stores."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    cdsapi_key: SecretStr | None = None
    cdsapi_config_file: str


class XHWISettings(BaseSettings):
    """Shared operational configuration for an XHWI source."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
        case_sensitive=False,
    )

    scientific_profile: Literal["xhwi-2024-v1"] = Field(validation_alias="SCIENTIFIC_PROFILE")
    xhwi_minimum: float = Field(validation_alias="XHWI_MINIMUM")
    numpy_dtype: FloatingDtype = Field(validation_alias="NUMPY_DTYPE")
    netcdf_engine: NetCDFEngine = Field(validation_alias="NETCDF_ENGINE")
    netcdf_format: NetCDFFormat = Field(validation_alias="NETCDF_FORMAT")
    netcdf_compression: bool = Field(validation_alias="NETCDF_COMPRESSION")
    netcdf_complevel: int = Field(validation_alias="NETCDF_COMPLEVEL", ge=0, le=9)
    netcdf_dtype: FloatingDtype = Field(validation_alias="NETCDF_DTYPE")
    netcdf_fill_value: float = Field(validation_alias="NETCDF_FILL_VALUE")
    netcdf_progress: bool = Field(validation_alias="NETCDF_PROGRESS")
    metadata_creators: str = Field(validation_alias="METADATA_CREATORS")
    metadata_institution: str = Field(validation_alias="METADATA_INSTITUTION")
    metadata_project: str = Field(validation_alias="METADATA_PROJECT")
    metadata_license: str = Field(validation_alias="METADATA_LICENSE")
    metadata_references: str = Field(validation_alias="METADATA_REFERENCES")
    metadata_repository: str = Field(validation_alias="METADATA_REPOSITORY")
    metadata_conventions: str = Field(validation_alias="METADATA_CONVENTIONS")
    metadata_processing_level: str = Field(validation_alias="METADATA_PROCESSING_LEVEL")
    log_level: LogLevel = Field(validation_alias="LOG_LEVEL")
    log_format: str = Field(validation_alias="LOG_FORMAT")

    device: Device
    torch_dtype: FloatingDtype
    latitude_block_size: int = Field(gt=0)
    longitude_block_size: int = Field(gt=0)
    months_to_run: list[int]
    calibration_start: str
    calibration_end: str
    application_start: str
    application_end: str
    calibration_policy: CalibrationPolicy
    part_existing_policy: ExistingFilePolicy
    final_existing_policy: ExistingFilePolicy
    concat_input_policy: ConcatInputPolicy
    calibration_file_template: str
    part_file_template: str
    final_file_template: str
    dataset_id: str
    source_id: str

    @field_validator("months_to_run")
    @classmethod
    def validate_months(cls, months: list[int]) -> list[int]:
        """Validate configured calendar months."""
        if (
            not months
            or len(months) != len(set(months))
            or any(month not in range(1, 13) for month in months)
        ):
            raise ValueError("months_to_run must contain unique values from 1 through 12")
        return months

    @model_validator(mode="after")
    def validate_templates(self) -> "XHWISettings":
        """Reject unknown placeholders and period-independent calibration names."""
        for template in (
            self.calibration_file_template,
            self.part_file_template,
            self.final_file_template,
        ):
            _validate_template(template)
        calibration_fields = _template_fields(self.calibration_file_template)
        if not {"start_year", "end_year"}.issubset(calibration_fields):
            raise ValueError("calibration_file_template must contain {start_year} and {end_year}")
        if "month" not in _template_fields(self.part_file_template):
            raise ValueError("part_file_template must contain {month}")
        _validate_safe_segment(self.source_id, "source_id")
        if self.netcdf_engine == "netcdf4" and self.netcdf_format not in {
            "NETCDF4",
            "NETCDF4_CLASSIC",
        }:
            raise ValueError("netcdf4 engine requires NETCDF4 or NETCDF4_CLASSIC format")
        if self.netcdf_engine == "scipy" and self.netcdf_format not in {
            "NETCDF3_64BIT",
            "NETCDF3_CLASSIC",
        }:
            raise ValueError("scipy engine requires a NETCDF3 format")
        if self.netcdf_engine == "scipy" and self.netcdf_compression:
            raise ValueError("scipy engine requires netcdf_compression=false")
        return self

    @property
    def calibration_period(self) -> tuple[str, str]:
        """Return the inclusive calibration date range."""
        return self.calibration_start, self.calibration_end

    @property
    def application_period(self) -> tuple[str | None, str | None]:
        """Return the configured application range, with empty bounds made open."""
        return self.application_start or None, self.application_end or None

    @property
    def torch_type(self) -> torch.dtype:
        """Return the configured PyTorch dtype."""
        return torch.float32 if self.torch_dtype is FloatingDtype.FLOAT32 else torch.float64

    @property
    def numpy_type(self) -> np.dtype[np.float32] | np.dtype[np.float64]:
        """Return the configured NumPy dtype."""
        return np.dtype(self.numpy_dtype.value)

    def resolve_device(self) -> torch.device:
        """Resolve the configured and available PyTorch device."""
        if self.device is Device.CUDA and not torch.cuda.is_available():
            raise RuntimeError("CUDA was explicitly requested but is not available.")
        if self.device is Device.AUTO:
            return torch.device("cuda" if torch.cuda.is_available() else "cpu")
        return torch.device(self.device.value)

    def render_path(self, template: str, **values: str | int) -> Path:
        """Render an operational path template with source identity and period values."""
        context: dict[str, str | int] = {
            "start_year": self.calibration_start[:4],
            "end_year": self.calibration_end[:4],
            "source": self.source_id.replace("-", ""),
            "model": "",
            "member": "",
            "grid": "",
            "scenario": "",
            "month": "",
        }
        context.update(values)
        return Path(template.format_map(context))

    @property
    def calibration_output(self) -> Path:
        """Return the calibration path for the effective calibration years."""
        return self.render_path(self.calibration_file_template)

    def part_output(self, month: int | str, *, scenario: str = "") -> Path:
        """Return a rendered monthly-part path."""
        rendered_month = f"{month:02d}" if isinstance(month, int) else month
        return self.render_path(self.part_file_template, month=rendered_month, scenario=scenario)

    def final_output(self, *, scenario: str = "") -> Path:
        """Return a rendered final-output path."""
        return self.render_path(self.final_file_template, scenario=scenario)

    def matching_parts(self, *, scenario: str = "") -> list[Path]:
        """Return all part files matching the configured template."""
        pattern = self.part_output("*", scenario=scenario)
        return sorted(pattern.parent.glob(pattern.name))


class ERA5Settings(XHWISettings):
    """ERA5 configuration loaded from the canonical environment."""

    model_config = SettingsConfigDict(
        env_prefix="ERA5_", env_file=".env", extra="ignore", populate_by_name=True
    )

    zarr_url: str
    zarr_chunks: str
    zarr_consolidated: bool
    request_timeout_seconds: float = Field(gt=0)
    variable_t2m: str
    variable_t2m_alias: str
    variable_humidity: str
    variable_humidity_alias: str
    latitude_start: float
    latitude_end: float
    longitude_start: float
    longitude_end: float

    @property
    def latitude_slice(self) -> slice:
        """Return the configured latitude selection."""
        return slice(self.latitude_start, self.latitude_end)

    @property
    def longitude_slice(self) -> slice:
        """Return the configured longitude selection."""
        return slice(self.longitude_start, self.longitude_end)


class ERA5LandSettings(ERA5Settings):
    """ERA5-Land configuration loaded from the canonical environment."""

    model_config = SettingsConfigDict(
        env_prefix="ERA5LAND_", env_file=".env", extra="ignore", populate_by_name=True
    )

    dewpoint_zarr_url: str


class CMIP6Settings(XHWISettings):
    """CMIP6 configuration loaded from the canonical environment."""

    model_config = SettingsConfigDict(
        env_prefix="CMIP6_", env_file=".env", extra="ignore", populate_by_name=True
    )

    model: str
    member: str
    grid: str
    scenarios: list[str]
    default_scenario: str
    time_chunk: int
    calibration_time_chunk: int
    spatial_chunk: int
    zarr_consolidated: bool
    variable_tas: str
    variable_huss: str
    variable_tasmax: str
    standard_pressure_pa: float
    interpolation_frequency: str
    interpolation_method: InterpolationMethod
    scenario_tas_template: str
    scenario_huss_template: str
    calibration_source_template: str

    @model_validator(mode="after")
    def validate_cmip6(self) -> "CMIP6Settings":
        """Validate scenarios and CMIP6 layout templates."""
        if self.default_scenario not in self.scenarios:
            raise ValueError("default_scenario must be included in scenarios")
        if len(self.scenarios) != len(set(self.scenarios)):
            raise ValueError("scenarios must contain unique values")
        for name, value in (
            ("source_id", self.source_id),
            ("model", self.model),
            ("member", self.member),
            ("grid", self.grid),
        ):
            _validate_safe_segment(value, name)
        for scenario in self.scenarios:
            _validate_safe_segment(scenario, "scenario")
        for template in (
            self.scenario_tas_template,
            self.scenario_huss_template,
            self.calibration_source_template,
        ):
            _validate_template(template)
        _require_template_fields(
            self.calibration_file_template,
            {"model", "start_year", "end_year"},
            "calibration_file_template",
        )
        for name, template in (
            ("part_file_template", self.part_file_template),
            ("final_file_template", self.final_file_template),
        ):
            _require_template_fields(template, {"model", "scenario", "member"}, name)
        for name, template in (
            ("scenario_tas_template", self.scenario_tas_template),
            ("scenario_huss_template", self.scenario_huss_template),
        ):
            _require_template_fields(template, {"model", "scenario", "member", "grid"}, name)
        _require_template_fields(
            self.calibration_source_template,
            {"model", "grid"},
            "calibration_source_template",
        )
        return self

    @override
    def render_path(self, template: str, **values: str | int) -> Path:
        """Render a CMIP6 path with model identity."""
        return super().render_path(
            template,
            model=self.model,
            member=self.member,
            grid=self.grid,
            **values,
        )

    @property
    def calibration_source(self) -> Path:
        """Return the configured native calibration input."""
        return self.render_path(self.calibration_source_template)

    def scenario_input(self, scenario: str, variable: str) -> Path:
        """Return a configured scenario input path for a named variable."""
        if variable == self.variable_tas:
            template = self.scenario_tas_template
        elif variable == self.variable_huss:
            template = self.scenario_huss_template
        else:
            raise ValueError(f"Unsupported configured CMIP6 variable: {variable}")
        return self.render_path(template, scenario=scenario)

    def monthly_parts_dir(self, scenario: str) -> Path:
        """Return the configured monthly-parts directory."""
        return self.part_output("*", scenario=scenario).parent

    def monthly_output(self, scenario: str) -> Path:
        """Return the configured final output path."""
        return self.final_output(scenario=scenario)


Settings = ERA5Settings


def _template_fields(template: str) -> set[str]:
    return {name for _, name, _, _ in Formatter().parse(template) if name is not None}


def _validate_template(template: str) -> None:
    unknown = _template_fields(template) - TEMPLATE_FIELDS
    if unknown:
        raise ValueError(f"Unknown path template placeholders: {sorted(unknown)}")


def _require_template_fields(template: str, required: set[str], name: str) -> None:
    missing = required - _template_fields(template)
    if missing:
        raise ValueError(f"{name} must contain placeholders: {sorted(missing)}")


def _validate_safe_segment(value: str, name: str) -> None:
    if not value or ".." in value or any(character in value for character in "/\\*?[]"):
        raise ValueError(f"{name} must be a safe path segment")
