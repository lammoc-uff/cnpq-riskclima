import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar

from riskclima_xhwi.config.settings import (
    CMIP6Scenario,
    CMIP6Settings,
    ERA5LandSettings,
    ERA5Settings,
    ExistingFilePolicy,
    XHWISettings,
)

LOGGER = logging.getLogger(__name__)


def build_monthly_output_dataset(
    monthly: xr.DataArray,
    settings: XHWISettings,
    *,
    scenario: CMIP6Scenario | None = None,
) -> xr.Dataset:
    """Build a metadata-rich monthly XHWI dataset.

    Parameters
    ----------
    monthly
        Monthly accumulated XHWI data.
    settings
        Runtime and scientific configuration.
    scenario
        CMIP6 experiment identifier when model data are being written.

    Returns
    -------
    xarray.Dataset
        Monthly XHWI dataset with CF-oriented metadata.
    """
    config = settings
    ds = monthly.to_dataset(name="xhwi_monthly_accumulated")
    creation_date = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_start = (
        _format_date(ds["time"].values[0])
        if "time" in ds.coords and ds.sizes.get("time", 0)
        else "unknown"
    )
    time_end = (
        _format_date(ds["time"].values[-1])
        if "time" in ds.coords and ds.sizes.get("time", 0)
        else "unknown"
    )
    lat_min = float(ds["lat"].min()) if "lat" in ds.coords else float("nan")
    lat_max = float(ds["lat"].max()) if "lat" in ds.coords else float("nan")
    lon_min = float(ds["lon"].min()) if "lon" in ds.coords else float("nan")
    lon_max = float(ds["lon"].max()) if "lon" in ds.coords else float("nan")
    if "time" in ds.coords:
        ds["time"].attrs.update({"standard_name": "time", "long_name": "Time", "axis": "T"})
    if "lat" in ds.coords:
        ds["lat"].attrs.update(
            {
                "standard_name": "latitude",
                "long_name": "Latitude",
                "units": "degrees_north",
                "axis": "Y",
            }
        )
    if "lon" in ds.coords:
        ds["lon"].attrs.update(
            {
                "standard_name": "longitude",
                "long_name": "Longitude",
                "units": "degrees_east",
                "axis": "X",
            }
        )
    ds["xhwi_monthly_accumulated"].attrs.update(
        {
            "long_name": "Monthly accumulated Extreme Heatwave Index",
            "units": "1",
            "cell_methods": "time: sum",
            "description": (
                "Monthly sum of daily XHWI products. Each daily product is the number "
                "of hours with nonzero XHWI multiplied by the daily sum of hourly XHWI."
            ),
        }
    )
    ds.attrs.update(
        {
            "summary": (
                "Monthly accumulated Extreme Heatwave Index. Spatial domain: lat "
                f"[{lat_min:.2f} deg, {lat_max:.2f} deg], lon [{lon_min:.2f} deg, "
                f"{lon_max:.2f} deg]. Temporal coverage: {time_start} to {time_end}. "
                "The calibration CDF is calendar-month-specific and grid-cell-specific."
            ),
            "creation_date": creation_date,
            "creator": config.metadata_creators,
            "references": config.metadata_references,
            "code_repository": config.metadata_repository,
            "institution": config.metadata_institution,
            "project": config.metadata_project,
            "license": config.metadata_license,
            "Conventions": config.metadata_conventions,
            "processing_level": config.metadata_processing_level,
            "dataset_id": config.dataset_id,
            "source_id": config.source_id,
            "calibration_period": (
                f"{config.calibration_period[0]} to {config.calibration_period[1]}"
            ),
            "calibration_method": ("Separate empirical CDF for each calendar month and grid cell."),
            "compute_backend": f"PyTorch on {config.resolve_device().type}",
            "scientific_profile": config.scientific_profile,
            "temperature_threshold_c": 32.0,
            "cdf_threshold_percent": 95.0,
            "xhwi_minimum": config.xhwi_minimum,
        }
    )
    ds.attrs.update(_source_metadata(config, creation_date, scenario))
    if "time" in ds.coords and ds.sizes.get("time", 0):
        calendar_months = np.unique(ds["time"].dt.month.values)
        ds.attrs["processed_calendar_months"] = ", ".join(
            f"{int(month):02d}" for month in calendar_months
        )
    return ds


def _source_metadata(
    settings: XHWISettings,
    creation_date: str,
    scenario: CMIP6Scenario | None,
) -> dict[str, str | float]:
    if isinstance(settings, CMIP6Settings):
        if scenario is None:
            raise ValueError("CMIP6 output metadata requires a scenario.")
        return {
            "title": f"Monthly accumulated XHWI for CMIP6 {settings.model} {scenario}",
            "keywords": "xhwi, extreme heatwave index, CMIP6, climate projection, RiskClima",
            "source": (
                f"CMIP6 model {settings.model}, experiment {scenario}, member {settings.member}, "
                f"grid {settings.grid}; {settings.variable_tas} and {settings.variable_huss} "
                f"interpolated to {settings.interpolation_frequency} with "
                f"{settings.interpolation_method.value}; {settings.variable_tasmax} from "
                f"{settings.calibration_source} used for calibration."
            ),
            "history": f"{creation_date} Computed CMIP6 XHWI using xarray and PyTorch blocks.",
            "model_id": settings.model,
            "experiment_id": scenario,
            "member_id": settings.member,
            "grid_label": settings.grid,
            "input_variables": (
                f"{settings.variable_tas}, {settings.variable_huss}, {settings.variable_tasmax}"
            ),
            "temporal_interpolation": (
                "sortby(time); resample(time="
                f"'{settings.interpolation_frequency}').interpolate("
                f"'{settings.interpolation_method.value}')"
            ),
            "humidity_method": (
                "Specific humidity converted to relative humidity using Bolton (1980) "
                f"saturation vapor pressure at p = {settings.standard_pressure_pa:g} Pa."
            ),
            "assumed_pressure_pa": settings.standard_pressure_pa,
            "calibration_source": str(settings.calibration_source),
        }
    if isinstance(settings, ERA5LandSettings):
        return {
            "title": "ERA5-Land - monthly accumulated Extreme Heatwave Index (XHWI)",
            "keywords": "xhwi, extreme heatwave index, ERA5-Land, reanalysis, RiskClima",
            "source": (
                f"ERA5-Land Zarr stores: {settings.variable_t2m} from {settings.zarr_url}; "
                f"{settings.variable_humidity} from {settings.dewpoint_zarr_url}."
            ),
            "history": f"{creation_date} Computed XHWI from ERA5-Land ARCO Zarr.",
            "input_variables": f"{settings.variable_t2m}, {settings.variable_humidity}",
            "humidity_method": "Relative humidity derived from temperature and dewpoint.",
            "application_period": (
                f"{settings.application_period[0]} to {settings.application_period[1]}"
            ),
            "calibration_source": settings.zarr_url,
        }
    if not isinstance(settings, ERA5Settings):
        raise TypeError("Unsupported XHWI settings type for source metadata.")
    return {
        "title": "ERA5 - monthly accumulated Extreme Heatwave Index (XHWI)",
        "keywords": "xhwi, extreme heatwave index, ERA5, reanalysis, RiskClima",
        "source": (
            f"ERA5 Zarr store {settings.zarr_url}. Variables: {settings.variable_t2m} and "
            f"{settings.variable_humidity}."
        ),
        "history": f"{creation_date} Computed XHWI from ERA5 ARCO Zarr.",
        "input_variables": f"{settings.variable_t2m}, {settings.variable_humidity}",
        "humidity_method": "Relative humidity derived from temperature and dewpoint.",
        "calibration_source": settings.zarr_url,
    }


def normalize_months(months: Sequence[int] | None = None) -> list[int]:
    """Validate, sort, and normalize calendar months.

    Parameters
    ----------
    months
        Calendar month numbers. Defaults to all months.

    Returns
    -------
    list of int
        Sorted month numbers.

    Raises
    ------
    ValueError
        If a month is outside 1 through 12 or appears more than once.
    """
    normalized = list(range(1, 13)) if months is None else list(months)
    invalid_months = [month for month in normalized if month < 1 or month > 12]
    if invalid_months:
        raise ValueError(f"Invalid months: {invalid_months}. Expected values from 1 to 12.")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"Duplicated months found: {normalized}")
    return sorted(normalized)


def write_calibration_netcdf(
    calibration: xr.DataArray,
    output_path: Path | str,
    *,
    settings: XHWISettings,
    policy: ExistingFilePolicy,
) -> Path:
    """Write a maximum-temperature calibration NetCDF file."""
    path, should_write = prepare_output_path(output_path, policy)
    if not should_write:
        return path
    calibration.attrs["calibration_period"] = (
        f"{settings.calibration_start} to {settings.calibration_end}"
    )
    if isinstance(settings, CMIP6Settings):
        calibration.attrs.update(model_id=settings.model, grid_label=settings.grid)
    calibration.encoding.update(_netcdf_encoding(settings))
    _write_netcdf(calibration, path, settings)
    return path


def write_monthly_netcdf(
    ds: xr.Dataset,
    output_path: Path | str,
    *,
    settings: XHWISettings,
    policy: ExistingFilePolicy,
) -> Path:
    """Write a sorted monthly XHWI NetCDF file."""
    ds = ds.sortby("time")
    if "time" in ds.indexes and not ds.indexes["time"].is_monotonic_increasing:
        raise ValueError("Output time coordinate is not monotonic increasing.")
    if "time" in ds.indexes and not ds.indexes["time"].is_unique:
        raise ValueError("Output time coordinate contains duplicated values.")
    path, should_write = prepare_output_path(output_path, policy)
    if not should_write:
        return path
    ds["xhwi_monthly_accumulated"].encoding = {}
    ds["xhwi_monthly_accumulated"].encoding.update(_netcdf_encoding(settings))
    _write_netcdf(ds, path, settings)
    return path


def concat_monthly_netcdfs(
    input_paths: Sequence[Path | str],
    output_path: Path | str,
    *,
    settings: XHWISettings,
    scenario: CMIP6Scenario | None = None,
    policy: ExistingFilePolicy,
) -> Path:
    """Concatenate partial monthly XHWI NetCDF files."""
    paths = [Path(path) for path in input_paths]
    if not paths:
        raise ValueError("No monthly part files were provided.")
    LOGGER.info("Concatenating %d monthly part files", len(paths))
    ds_raw = xr.open_mfdataset(
        paths, combine="nested", concat_dim="time", engine=settings.netcdf_engine
    )
    try:
        ds_raw = ds_raw.sortby("time")
        if "time" in ds_raw.indexes and not ds_raw.indexes["time"].is_unique:
            duplicated_times = ds_raw.indexes["time"][ds_raw.indexes["time"].duplicated()]
            raise ValueError(
                "Duplicated time values found during concatenation. "
                f"First duplicated values: {duplicated_times[:10]}"
            )
        ds_final = build_monthly_output_dataset(
            ds_raw["xhwi_monthly_accumulated"], settings=settings, scenario=scenario
        )
        ds_final.attrs["source_monthly_part_files"] = "; ".join(str(path) for path in paths)
        return write_monthly_netcdf(ds_final, output_path, settings=settings, policy=policy)
    finally:
        ds_raw.close()


def prepare_output_path(output_path: Path | str, policy: ExistingFilePolicy) -> tuple[Path, bool]:
    """Prepare an output path according to its configured existing-file policy."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return path, True
    if policy is ExistingFilePolicy.SKIP:
        LOGGER.info("File already exists; skipping: %s", path)
        return path, False
    if policy is ExistingFilePolicy.OVERWRITE:
        path.unlink()
        return path, True
    raise FileExistsError(f"File already exists: {path}")


def _write_netcdf(data: xr.Dataset | xr.DataArray, path: Path, settings: XHWISettings) -> None:
    if settings.netcdf_progress:
        with ProgressBar():
            data.to_netcdf(path, engine=settings.netcdf_engine, format=settings.netcdf_format)
        return
    data.to_netcdf(path, engine=settings.netcdf_engine, format=settings.netcdf_format)


def _netcdf_encoding(settings: XHWISettings) -> dict[str, str | float | bool | int]:
    encoding: dict[str, str | float | bool | int] = {
        "_FillValue": settings.netcdf_fill_value,
        "dtype": settings.netcdf_dtype.value,
    }
    if settings.netcdf_engine == "netcdf4":
        encoding.update(zlib=settings.netcdf_compression, complevel=settings.netcdf_complevel)
    return encoding


def _format_date(value: np.datetime64 | object) -> str:
    """Format NumPy and CF calendar dates for dataset metadata."""
    if isinstance(value, np.datetime64):
        return np.datetime_as_string(value, unit="D")
    return str(value)[:10]
