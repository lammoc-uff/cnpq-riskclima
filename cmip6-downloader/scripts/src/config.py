"""Runtime configuration loaded from the project ``.env`` file."""

from __future__ import annotations

import re
from datetime import date
from enum import StrEnum
from pathlib import Path
from string import Formatter
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]
GROUP_FIELDS = ("source_id", "experiment_id", "table_id", "variable_id", "grid_label")
ZARR_FORMAT_VERSION = 2
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
type CalendarName = Literal[
    "360_day",
    "365_day",
    "366_day",
    "all_leap",
    "gregorian",
    "julian",
    "noleap",
    "proleptic_gregorian",
    "standard",
]


class ExistingPolicy(StrEnum):
    """Policy applied when a local Zarr store already exists."""

    SKIP = "skip"
    OVERWRITE = "overwrite"
    FAIL = "fail"


class EnsembleMode(StrEnum):
    """Ensemble products to create after member downloads."""

    NONE = "none"
    STACK = "stack"
    MEAN = "mean"
    BOTH = "both"


class Alignment(StrEnum):
    """Coordinate alignment used to combine ensemble members."""

    INNER = "inner"
    OUTER = "outer"


class CurvilinearPolicy(StrEnum):
    """Handling of datasets with two-dimensional spatial coordinates."""

    KEEP_GLOBAL = "keep_global"
    REJECT = "reject"


class LogLevel(StrEnum):
    """Supported Python logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Validated operational settings.

    A new instance reads ``.env`` again; no process-level cache is used.
    """

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        env_ignore_empty=False,
        case_sensitive=True,
        extra="forbid",
        alias_generator=str.upper,
        validate_by_name=True,
    )

    catalog_aws_path: Path
    catalog_google_path: Path
    filtered_catalog_dir: Path
    preferred_catalog_path: Path
    downloads_dir: Path

    provider_priority: list[str]
    aws_anonymous: bool
    google_anonymous: bool

    source_ids: list[str]
    experiment_ids: list[str]
    table_ids: list[str]
    variable_ids: list[str]
    grid_labels: list[str]
    member_ids: list[str]

    historical_start: date | None
    historical_end: date | None
    historical_experiments: list[str]
    future_experiments: list[str]
    future_start: date
    future_end: date

    latitude_min: float
    latitude_max: float
    longitude_min: float
    longitude_max: float
    spatial_subset: bool
    excluded_variables: list[str]
    curvilinear_policy: CurvilinearPolicy

    calendar_conversion: bool
    target_calendar: CalendarName
    calendar_align_on: Literal["date", "year"]
    convert_datetime_index: bool
    drop_duplicate_times: bool

    max_workers: Annotated[int, Field(gt=0)]
    time_chunk_size: Annotated[int, Field(gt=0)]
    open_chunks: dict[str, Annotated[int, Field(gt=0)]]
    remote_consolidated: bool
    output_consolidated: bool
    existing_policy: ExistingPolicy

    ensemble_mode: EnsembleMode
    ensemble_alignment: Alignment
    ensemble_dimension_chunks: dict[str, Annotated[int, Field(gt=0)]]
    ensemble_default_chunk_size: Annotated[int, Field(gt=0)]
    cleanup_members: bool
    member_store_template: str

    aws_only_catalog_filename: str
    google_only_catalog_filename: str
    provider_decisions_filename: str
    group_catalog_filename: str
    group_log_filename: str
    global_log_filename: str
    ensemble_all_filename: str
    ensemble_mean_filename: str
    log_level: LogLevel
    log_format: str

    @field_validator("historical_start", "historical_end", mode="before")
    @classmethod
    def empty_date_is_none(cls, value: object) -> object:
        """Interpret explicitly empty historical bounds as unbounded."""
        return None if value == "" else value

    @field_validator(
        "source_ids",
        "experiment_ids",
        "table_ids",
        "variable_ids",
        "grid_labels",
        "historical_experiments",
        "future_experiments",
    )
    @classmethod
    def validate_required_lists(cls, value: list[str]) -> list[str]:
        """Reject empty identifiers in required filter lists."""
        if not value or any(not item.strip() for item in value):
            raise ValueError("required filter lists must contain non-empty values")
        return value

    @field_validator("member_ids", "excluded_variables")
    @classmethod
    def validate_optional_lists(cls, value: list[str]) -> list[str]:
        """Reject blank entries while allowing an empty list."""
        if any(not item.strip() for item in value):
            raise ValueError("optional lists cannot contain blank values")
        return value

    @field_validator("provider_priority")
    @classmethod
    def validate_provider_priority(cls, value: list[str]) -> list[str]:
        """Require each supported provider exactly once."""
        if len(value) != 2 or set(value) != {"aws", "google"}:
            raise ValueError("provider priority must contain aws and google exactly once")
        return value

    @field_validator("open_chunks", "ensemble_dimension_chunks")
    @classmethod
    def validate_chunk_dimensions(cls, value: dict[str, int]) -> dict[str, int]:
        """Require safe dimension names for configured chunk mappings."""
        if any(SAFE_NAME.fullmatch(name) is None for name in value):
            raise ValueError("chunk dimension names must be safe identifiers")
        return value

    @field_validator("ensemble_dimension_chunks")
    @classmethod
    def reject_time_ensemble_chunk(cls, value: dict[str, int]) -> dict[str, int]:
        """Reserve time chunking for TIME_CHUNK_SIZE."""
        if "time" in value:
            raise ValueError("ENSEMBLE_DIMENSION_CHUNKS cannot contain time")
        return value

    @field_validator("member_store_template")
    @classmethod
    def validate_member_store_template(cls, value: str) -> str:
        """Require one member field in a plain Zarr filename template."""
        if not value or Path(value).name != value or value in {".", ".."}:
            raise ValueError("MEMBER_STORE_TEMPLATE must be a plain filename")
        try:
            parsed = list(Formatter().parse(value))
        except ValueError as error:
            raise ValueError("MEMBER_STORE_TEMPLATE is not a valid format string") from error
        fields = [
            (field, format_spec, conversion)
            for _, field, format_spec, conversion in parsed
            if field is not None
        ]
        if fields != [("member_id", "", None)]:
            raise ValueError("MEMBER_STORE_TEMPLATE must contain exactly one {member_id} field")
        if not value.endswith(".zarr"):
            raise ValueError("MEMBER_STORE_TEMPLATE must have the .zarr suffix")
        return value

    @field_validator(
        "catalog_aws_path",
        "catalog_google_path",
        "filtered_catalog_dir",
        "preferred_catalog_path",
        "downloads_dir",
    )
    @classmethod
    def validate_path(cls, value: Path) -> Path:
        """Reject empty and parent-traversing operational paths."""
        if not str(value).strip() or ".." in value.parts:
            raise ValueError("paths must be non-empty and cannot contain '..'")
        return value

    @field_validator(
        "aws_only_catalog_filename",
        "google_only_catalog_filename",
        "provider_decisions_filename",
        "group_catalog_filename",
        "group_log_filename",
        "global_log_filename",
        "ensemble_all_filename",
        "ensemble_mean_filename",
    )
    @classmethod
    def validate_filename(cls, value: str) -> str:
        """Require a plain filename rather than a path."""
        if not value or Path(value).name != value or value in {".", ".."}:
            raise ValueError("configured filenames must be plain filenames")
        return value

    @model_validator(mode="after")
    def validate_invariants(self) -> Settings:
        """Validate periods, bounds, experiment sets, and output collisions."""
        if self.latitude_min >= self.latitude_max or not (-90 <= self.latitude_min <= 90):
            raise ValueError("latitude bounds must be ordered within [-90, 90]")
        if not (-90 <= self.latitude_max <= 90):
            raise ValueError("latitude bounds must be ordered within [-90, 90]")
        if self.longitude_min >= self.longitude_max or not (-180 <= self.longitude_min <= 180):
            raise ValueError("longitude bounds must be ordered within [-180, 180]")
        if not (-180 <= self.longitude_max <= 180):
            raise ValueError("longitude bounds must be ordered within [-180, 180]")
        if (
            self.historical_start
            and self.historical_end
            and self.historical_start > self.historical_end
        ):
            raise ValueError("historical start must not be after historical end")
        if self.future_start > self.future_end:
            raise ValueError("future start must not be after future end")
        historical = set(self.historical_experiments)
        future = set(self.future_experiments)
        experiments = set(self.experiment_ids)
        if not historical <= experiments:
            raise ValueError("HISTORICAL_EXPERIMENTS must be a subset of EXPERIMENT_IDS")
        if not future <= experiments:
            raise ValueError("FUTURE_EXPERIMENTS must be a subset of EXPERIMENT_IDS")
        if historical & future:
            raise ValueError("HISTORICAL_EXPERIMENTS and FUTURE_EXPERIMENTS must be disjoint")
        self._validate_output_collisions()
        return self

    def _validate_output_collisions(self) -> None:
        group_names = {
            self.group_catalog_filename,
            self.group_log_filename,
            self.ensemble_all_filename,
            self.ensemble_mean_filename,
        }
        if len(group_names) != 4:
            raise ValueError("group catalog, log, and ensemble filenames must all be distinct")

        filtered_names = {
            self.aws_only_catalog_filename,
            self.google_only_catalog_filename,
            self.provider_decisions_filename,
        }
        if len(filtered_names) != 3:
            raise ValueError("filtered catalog output filenames must all be distinct")

        filtered_dir = self.resolve_path(self.filtered_catalog_dir).resolve()
        preferred = self.resolve_path(self.preferred_catalog_path).resolve()
        if preferred.parent == filtered_dir and preferred.name in filtered_names:
            raise ValueError("preferred catalog filename collides with a filtered catalog output")

        catalog_paths = {
            self.resolve_path(self.catalog_aws_path).resolve(),
            self.resolve_path(self.catalog_google_path).resolve(),
            preferred,
        }
        if len(catalog_paths) != 3:
            raise ValueError("AWS, Google, and preferred catalog paths must all be distinct")

    def resolve_path(self, path: Path) -> Path:
        """Resolve a configured path relative to the project root."""
        return path if path.is_absolute() else BASE_DIR / path

    def format_member_store(self, member_id: str) -> str:
        """Format a safe member store filename from a validated identifier."""
        if SAFE_NAME.fullmatch(member_id) is None:
            raise ValueError(f"unsafe member identifier: {member_id}")
        filename = self.member_store_template.format(member_id=member_id)
        if Path(filename).name != filename or not filename.endswith(".zarr"):
            raise ValueError(f"member store template produced an unsafe filename: {filename}")
        return filename


def settings_with_overrides(
    settings: Settings,
    overrides: dict[str, str | int | float | bool | list[str] | Path | date | None],
) -> Settings:
    """Return validated settings with explicit CLI values applied."""
    values = settings.model_dump()
    values.update({key: value for key, value in overrides.items() if value is not None})
    return Settings.model_validate(values)
