from __future__ import annotations

import logging
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import cdsapi
import xarray as xr
from cdsapi.api import Client as RawCDSClient

from riskclima_spi.config import CDSCredentials, ERA5Settings
from riskclima_spi.pipeline import (
    SPISourceMetadata,
    build_output_dataset,
    calculate_spi,
    write_output,
)

type CDSRequestValue = str | list[str] | list[float]
type CDSRequest = dict[str, CDSRequestValue]

LOGGER = logging.getLogger(__name__)


class CDSClient(Protocol):
    """Required CDS API client interface."""

    def download(self, dataset: str, request: CDSRequest, target: str) -> None: ...


class _RawCDSClient(Protocol):
    """Target-aware retrieval supported by CDS API client implementations."""

    def retrieve(self, dataset: str, request: CDSRequest, target: str) -> object: ...


class CDSClientAdapter:
    """Typed boundary around the cdsapi client."""

    def __init__(self, client: RawCDSClient | _RawCDSClient) -> None:
        self._client = client

    def download(self, dataset: str, request: CDSRequest, target: str) -> None:
        """Retrieve one request directly to an explicit local path."""
        self._client.retrieve(dataset, request, target)


@dataclass(frozen=True)
class ERA5RequestPart:
    """One temporary portion of an ERA5 download."""

    request: CDSRequest
    path: Path


def standardize_era5_dims(dataset: xr.Dataset, settings: ERA5Settings) -> xr.Dataset:
    """Normalize ERA5 dimensions and sort all coordinates increasingly.

    Parameters
    ----------
    dataset
        ERA5 dataset using configured CDS dimension names or canonical names.
    settings
        Configured source dimension names.

    Returns
    -------
    xarray.Dataset
        Dataset using time, lat, and lon dimensions in increasing order.
    """
    configured_names = {
        settings.era5_time_dimension: "time",
        settings.era5_latitude_dimension: "lat",
        settings.era5_longitude_dimension: "lon",
    }
    rename = {
        source: target
        for source, target in configured_names.items()
        if source != target and (source in dataset.dims or source in dataset.coords)
    }
    standardized = dataset.rename(rename) if rename else dataset
    missing = [
        dimension for dimension in ("time", "lat", "lon") if dimension not in standardized.dims
    ]
    if missing:
        raise ValueError(f"ERA5 dataset is missing dimensions: {missing}")
    standardized = _merge_expver(standardized)
    standardized = standardized.drop_vars(
        [
            name
            for name in ("expver", "number")
            if name in standardized.coords or name in standardized.dims
        ],
        errors="ignore",
    )
    return standardized.sortby(["time", "lat", "lon"])


def _merge_expver(dataset: xr.Dataset) -> xr.Dataset:
    """Collapse the ERA5/ERA5T blend dimension when present.

    Parameters
    ----------
    dataset
        Dataset that may carry an ``expver`` dimension mixing finalized ERA5
        and preliminary ERA5T values.

    Returns
    -------
    xarray.Dataset
        Dataset without ``expver``. Finalized ERA5 values take priority over
        ERA5T where both are non-missing.

    Raises
    ------
    ValueError
        If ``expver`` has more than two versions or if a cell has conflicting
        non-missing values across versions.
    """
    if "expver" not in dataset.dims:
        return dataset
    if dataset.sizes["expver"] == 1:
        return dataset.isel(expver=0, drop=True)
    if dataset.sizes["expver"] > 2:
        raise ValueError("ERA5 dataset has more than two expver versions")
    primary = dataset.isel(expver=0, drop=True)
    secondary = dataset.isel(expver=1, drop=True)
    conflict = primary.notnull() & secondary.notnull() & (primary != secondary)
    if bool(conflict.to_array().any().item()):
        raise ValueError("ERA5 dataset has conflicting non-missing values across expver versions")
    return primary.where(primary.notnull(), secondary)


def prepare_era5_monthly_precipitation(dataset: xr.Dataset, settings: ERA5Settings) -> xr.DataArray:
    """Convert ERA5 mean daily precipitation into monthly accumulation.

    Parameters
    ----------
    dataset
        ERA5 monthly averaged reanalysis dataset.
    settings
        ERA5 variable and dimension configuration.

    Returns
    -------
    xarray.DataArray
        Monthly accumulated precipitation in millimetres.
    """
    standardized = standardize_era5_dims(dataset, settings)
    variable = settings.era5_precipitation_variable
    if variable not in standardized:
        raise ValueError(f"ERA5 precipitation variable {variable!r} is not present")
    precipitation = standardized[variable]
    if not {"time", "lat", "lon"}.issubset(precipitation.dims):
        raise ValueError("ERA5 precipitation must use time, lat, and lon dimensions")
    units = precipitation.attrs.get("units", "")
    if units not in {"m", "m/day", "m day-1", "m of water equivalent"}:
        raise ValueError("ERA5 monthly averaged total precipitation units must be metres per day")
    monthly = precipitation * 1000 * precipitation["time"].dt.days_in_month
    return monthly.rename("pr").assign_attrs(units="mm month-1")


def ensure_era5_input(settings: ERA5Settings) -> Path:
    """Download and atomically replace the configured ERA5 input.

    Parameters
    ----------
    settings
        ERA5 acquisition contract and final raw-data path template.

    Returns
    -------
    pathlib.Path
        Newly created final ERA5 NetCDF file.
    """
    credentials = CDSCredentials()
    return ensure_era5_input_with_client(settings, _create_cds_client(credentials))


def ensure_era5_input_with_client(settings: ERA5Settings, client: CDSClient) -> Path:
    """Download ERA5 input using an explicit CDS client.

    Parameters
    ----------
    settings
        ERA5 acquisition contract and final raw-data path template.
    client
        CDS client used for every configured request.

    Returns
    -------
    pathlib.Path
        Newly created final ERA5 NetCDF file.
    """
    requests = _build_request_parts(settings)
    output_path = settings.raw_input_path()
    parts = _part_paths(output_path, requests)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(f"{output_path.suffix}.tmp")
    try:
        for part in parts:
            LOGGER.info("Requesting ERA5 data: %s", part.path.name)
            client.download(settings.era5_dataset, part.request, str(part.path))
        _concatenate_parts(parts, temporary_output, settings)
        temporary_output.replace(output_path)
    finally:
        temporary_output.unlink(missing_ok=True)
        for part in parts:
            part.path.unlink(missing_ok=True)
    LOGGER.info("ERA5 input file created successfully: %s", output_path)
    return output_path


def run_era5(settings: ERA5Settings) -> Path:
    """Acquire ERA5 data and calculate SPI.

    Parameters
    ----------
    settings
        ERA5 acquisition, SPI, output, and metadata configuration.

    Returns
    -------
    pathlib.Path
        Written SPI NetCDF path.
    """
    input_path = ensure_era5_input(settings)
    chunks = {
        "time": -1,
        "lat": settings.era5_spatial_chunk,
        "lon": settings.era5_spatial_chunk,
    }
    with xr.open_dataset(input_path, chunks=chunks) as dataset:
        monthly = prepare_era5_monthly_precipitation(dataset, settings)
        spi = calculate_spi(monthly, monthly, settings)
        output = build_output_dataset(
            spi,
            settings,
            source_metadata=SPISourceMetadata(
                title="Standardized Precipitation Index for ERA5",
                source=f"ERA5 {settings.era5_product_type} total precipitation",
                keywords="spi, standardized precipitation index, ERA5, reanalysis, RiskClima",
                input_variables=settings.era5_precipitation_variable,
                input_frequency="monthly",
                precipitation_conversion=(
                    "ERA5 monthly averaged total precipitation represents an accumulation "
                    "with an effective processing period of one day. Values in metres per "
                    "day were multiplied by 1000 and by the number of days in each calendar "
                    "month, producing monthly accumulated precipitation in mm month-1."
                ),
            ),
        )
        output.attrs.update(
            dataset_id=settings.era5_dataset,
            source_id="era5",
            product_type=settings.era5_product_type,
        )
        return write_output(
            output,
            settings.output_path(),
            settings,
            dask_workers=settings.era5_dask_workers,
        )


def _build_request_parts(settings: ERA5Settings) -> list[CDSRequest]:
    complete_end_year = (
        settings.era5_download_end.year
        if settings.era5_download_end.month == 12
        else settings.era5_download_end.year - 1
    )
    requests: list[CDSRequest] = []
    if settings.era5_download_start.year <= complete_end_year:
        requests.append(
            _build_request(
                settings,
                years=range(settings.era5_download_start.year, complete_end_year + 1),
                months=range(1, 13),
            )
        )
    if settings.era5_download_end.month < 12:
        requests.append(
            _build_request(
                settings,
                years=range(settings.era5_download_end.year, settings.era5_download_end.year + 1),
                months=range(1, settings.era5_download_end.month + 1),
            )
        )
    return requests


def _build_request(settings: ERA5Settings, *, years: range, months: range) -> CDSRequest:
    return {
        "product_type": [settings.era5_product_type],
        "variable": [settings.era5_request_variable],
        "year": [str(year) for year in years],
        "month": [f"{month:02d}" for month in months],
        "time": [settings.era5_time],
        "data_format": settings.era5_data_format,
        "download_format": settings.era5_download_format,
        "area": [
            settings.era5_latitude_max,
            settings.era5_longitude_min,
            settings.era5_latitude_min,
            settings.era5_longitude_max,
        ],
    }


def _part_paths(output_path: Path, requests: list[CDSRequest]) -> list[ERA5RequestPart]:
    labels = ["complete-years", "current-year"] if len(requests) == 2 else ["complete-period"]
    return [
        ERA5RequestPart(request=request, path=output_path.with_suffix(f".{label}.part.nc"))
        for request, label in zip(requests, labels, strict=True)
    ]


def _concatenate_parts(
    parts: list[ERA5RequestPart],
    output_path: Path,
    settings: ERA5Settings,
) -> None:
    with ExitStack() as stack:
        datasets = [
            standardize_era5_dims(stack.enter_context(xr.open_dataset(part.path)), settings)
            for part in parts
        ]
        _validate_exact_spatial_grids(datasets)
        combined = xr.concat(datasets, dim="time", join="exact").sortby(["time", "lat", "lon"])
        _validate_monthly_coverage(combined, settings)
        combined.attrs.update(
            era5_dataset=settings.era5_dataset,
            era5_product_type=settings.era5_product_type,
            era5_request_variable=settings.era5_request_variable,
            era5_download_start=settings.era5_download_start.isoformat(),
            era5_download_end=settings.era5_download_end.isoformat(),
            era5_area=(
                f"{settings.era5_latitude_max}, {settings.era5_longitude_min}, "
                f"{settings.era5_latitude_min}, {settings.era5_longitude_max}"
            ),
        )
        combined.to_netcdf(
            output_path, engine=settings.netcdf_engine, format=settings.netcdf_format
        )


def _validate_exact_spatial_grids(datasets: list[xr.Dataset]) -> None:
    reference = datasets[0]
    for dataset in datasets[1:]:
        if not reference["lat"].equals(dataset["lat"]) or not reference["lon"].equals(
            dataset["lon"]
        ):
            raise ValueError(
                "ERA5 request parts must have exactly equal latitude and longitude grids"
            )


def _validate_monthly_coverage(dataset: xr.Dataset, settings: ERA5Settings) -> None:
    years = dataset["time"].dt.year.values.tolist()
    months = dataset["time"].dt.month.values.tolist()
    actual = [(int(year), int(month)) for year, month in zip(years, months, strict=True)]
    expected = _expected_months(settings)
    if actual != expected:
        raise ValueError("ERA5 input does not contain the complete configured monthly period")


def _expected_months(settings: ERA5Settings) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    year = settings.era5_download_start.year
    month = settings.era5_download_start.month
    end = (settings.era5_download_end.year, settings.era5_download_end.month)
    while (year, month) <= end:
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def _create_cds_client(credentials: CDSCredentials) -> CDSClient:
    configured_key = credentials.cdsapi_key
    if configured_key is not None and configured_key.get_secret_value().strip():
        return CDSClientAdapter(
            cdsapi.Client(
                url=credentials.cdsapi_url,
                key=configured_key.get_secret_value().strip(),
            )
        )
    key = _read_cdsapi_config_key(credentials.cdsapi_config_file)
    return CDSClientAdapter(cdsapi.Client(url=credentials.cdsapi_url, key=key))


def _read_cdsapi_config_key(path: Path) -> str:
    expanded_path = path.expanduser()
    if not expanded_path.is_file():
        raise ValueError(
            "CDS API credentials are unavailable. Set CDSAPI_KEY or provide CDSAPI_CONFIG_FILE."
        )
    values: dict[str, str] = {}
    for line in expanded_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip() == "key":
            values[key.strip()] = value.strip()
    if not values.get("key"):
        raise ValueError("CDS API configuration file must contain a nonblank key")
    return values["key"]
