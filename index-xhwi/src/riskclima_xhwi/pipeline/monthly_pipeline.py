import logging
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import xarray as xr
from numpy.typing import NDArray

from riskclima_xhwi.config.settings import (
    CMIP6Scenario,
    CMIP6Settings,
    ERA5LandSettings,
    ERA5Settings,
    ExistingFilePolicy,
    XHWISettings,
)
from riskclima_xhwi.io.writers import (
    build_monthly_output_dataset,
    normalize_months,
    write_calibration_netcdf,
)
from riskclima_xhwi.pipeline.block_processor import process_month_block_torch
from riskclima_xhwi.pipeline.data_access import (
    iter_spatial_blocks,
    open_calibration_tasmax_from_t2m,
    open_cmip6_calibration,
    open_cmip6_hourly_inputs,
    open_era5_inputs,
    open_era5_land_inputs,
    open_t2m_calibration_inputs,
)
from riskclima_xhwi.pipeline.policies import resolve_calibration
from riskclima_xhwi.preprocessing.era5 import open_saved_tasmax_calibration

LOGGER = logging.getLogger(__name__)


def compute_monthly_xhwi_torch(
    tas_c: xr.DataArray,
    hurs: xr.DataArray,
    tasmax_calibration: xr.DataArray,
    *,
    months: Sequence[int],
    settings: XHWISettings,
    source_label: str,
    scenario: CMIP6Scenario | None = None,
) -> xr.Dataset:
    """Compute monthly XHWI from prepared inputs using spatial PyTorch blocks.

    Parameters
    ----------
    tas_c
        Hourly air temperature in degrees Celsius.
    hurs
        Hourly relative humidity in percent.
    tasmax_calibration
        Native or derived daily maximum calibration temperature.
    months
        Calendar months to process.
    settings
        Shared scientific and runtime configuration.
    source_label
        Source name used in logs and errors.
    scenario
        CMIP6 scenario when processing model data.

    Returns
    -------
    xarray.Dataset
        Monthly accumulated XHWI with source-specific metadata.
    """
    selected_months = normalize_months(months)
    tas_c, hurs, tasmax_calibration = _prepare_aligned_inputs(tas_c, hurs, tasmax_calibration)
    lat_values = tas_c["lat"].values
    lon_values = tas_c["lon"].values
    lat_size = tas_c.sizes["lat"]
    lon_size = tas_c.sizes["lon"]
    n_blocks = math.ceil(lat_size / settings.latitude_block_size) * math.ceil(
        lon_size / settings.longitude_block_size
    )
    month_arrays: list[NDArray[np.float32] | NDArray[np.float64]] = []
    month_times: list[NDArray[np.generic]] = []
    for month in selected_months:
        LOGGER.info(
            "Processing %s calendar month %02d in %d spatial blocks",
            source_label,
            month,
            n_blocks,
        )
        tas_month = tas_c.sel(time=tas_c["time.month"] == month)
        hurs_month = hurs.sel(time=hurs["time.month"] == month)
        calibration_month = tasmax_calibration.sel(
            calibration_time=tasmax_calibration["calibration_time.month"] == month
        )
        if tas_month.sizes.get("time", 0) == 0:
            LOGGER.warning("No hourly %s data found for month %02d; skipping", source_label, month)
            continue
        if calibration_month.sizes.get("calibration_time", 0) == 0:
            raise ValueError(f"No calibration tasmax data found for month {month}.")
        month_template: NDArray[np.float32] | NDArray[np.float64] | None = None
        month_time: NDArray[np.generic] | None = None
        blocks = iter_spatial_blocks(
            lat_size,
            lon_size,
            settings.latitude_block_size,
            settings.longitude_block_size,
        )
        for lat_slice, lon_slice in blocks:
            block_np, block_time = process_month_block_torch(
                tas_month,
                hurs_month,
                calibration_month,
                lat_slice,
                lon_slice,
                settings,
            )
            if month_template is None:
                month_time = block_time
                shape = (len(month_time), lat_size, lon_size)
                if settings.numpy_dtype.value == "float32":
                    month_template = np.full(shape, np.nan, dtype=np.float32)
                else:
                    month_template = np.full(shape, np.nan, dtype=np.float64)
            elif month_time is not None and len(block_time) != len(month_time):
                raise ValueError("Inconsistent monthly time length across spatial blocks.")
            if month_template is None:
                raise RuntimeError("Monthly output block was not initialized.")
            month_template[:, lat_slice, lon_slice] = block_np
        if month_template is not None and month_time is not None:
            month_arrays.append(month_template)
            month_times.append(month_time)
    if not month_arrays:
        raise ValueError(f"No monthly XHWI outputs were generated for {source_label}.")
    monthly = xr.DataArray(
        np.concatenate(month_arrays, axis=0),
        dims=("time", "lat", "lon"),
        coords={
            "time": np.concatenate(month_times, axis=0),
            "lat": lat_values,
            "lon": lon_values,
        },
        name="xhwi_monthly_accumulated",
    ).sortby("time")
    return build_monthly_output_dataset(monthly, settings, scenario=scenario)


def _prepare_aligned_inputs(
    tas_c: xr.DataArray,
    hurs: xr.DataArray,
    tasmax_calibration: xr.DataArray,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """Align and validate application and calibration arrays before tensor conversion."""
    try:
        tas_c, hurs = xr.align(tas_c, hurs, join="exact")
    except ValueError as error:
        raise ValueError("Application tas and hurs coordinates must match exactly.") from error
    try:
        tas_c = tas_c.transpose("time", "lat", "lon")
        hurs = hurs.transpose("time", "lat", "lon")
        tasmax_calibration = tasmax_calibration.transpose("calibration_time", "lat", "lon")
    except ValueError as error:
        raise ValueError(
            "Application inputs must have dimensions (time, lat, lon) and calibration must "
            "have dimensions (calibration_time, lat, lon)."
        ) from error
    for coordinate in ("lat", "lon"):
        if not tas_c.get_index(coordinate).equals(tasmax_calibration.get_index(coordinate)):
            raise ValueError(
                f"Application and calibration {coordinate} coordinates must match exactly; "
                "spatial regridding is not performed."
            )
    return tas_c, hurs, tasmax_calibration


def compute_era5_monthly_xhwi_torch(
    path: str,
    months: Sequence[int] | None = None,
    settings: ERA5Settings | None = None,
) -> xr.Dataset:
    """Open ERA5 data and compute monthly XHWI."""
    config = settings or ERA5Settings()
    tas_c, hurs = open_era5_inputs(path, config)
    calibration = _era5_calibration(path, config)
    return compute_monthly_xhwi_torch(
        tas_c,
        hurs,
        calibration,
        months=config.months_to_run if months is None else months,
        settings=config,
        source_label="ERA5",
    )


def compute_era5land_monthly_xhwi_torch(
    path: str,
    months: Sequence[int] | None = None,
    settings: ERA5LandSettings | None = None,
) -> xr.Dataset:
    """Open ERA5-Land data for the application period and compute monthly XHWI."""
    config = settings or ERA5LandSettings()
    tas_c, hurs = open_era5_land_inputs(path, config)
    calibration = _era5_calibration(path, config)
    return compute_monthly_xhwi_torch(
        tas_c,
        hurs,
        calibration,
        months=config.months_to_run if months is None else months,
        settings=config,
        source_label="ERA5-Land",
    )


def compute_cmip6_monthly_xhwi_torch(
    scenario: CMIP6Scenario,
    months: Sequence[int] | None = None,
    settings: CMIP6Settings | None = None,
) -> xr.Dataset:
    """Open one CMIP6 scenario and compute monthly XHWI."""
    config = settings or CMIP6Settings()
    tas_c, hurs = open_cmip6_hourly_inputs(scenario, config)
    calibration = _cmip6_calibration(config)
    return compute_monthly_xhwi_torch(
        tas_c,
        hurs,
        calibration,
        months=config.months_to_run if months is None else months,
        settings=config,
        source_label=f"CMIP6 {config.model} {scenario}",
        scenario=scenario,
    )


def _era5_calibration(
    source: str,
    settings: ERA5Settings,
) -> xr.DataArray:
    def create() -> Path:
        calibration_tas = open_t2m_calibration_inputs(source, settings)
        calibration = open_calibration_tasmax_from_t2m(calibration_tas, settings)
        calibration.name = "tasmax_calibration"
        calibration.attrs["calibration_period"] = (
            f"{settings.calibration_start} to {settings.calibration_end}"
        )
        return write_calibration_netcdf(
            calibration,
            settings.calibration_output,
            settings=settings,
            policy=ExistingFilePolicy.OVERWRITE,
        )

    resolved = resolve_calibration(settings, create)
    if resolved is not None:
        return open_saved_tasmax_calibration(resolved, settings)
    calibration_tas = open_t2m_calibration_inputs(source, settings)
    return open_calibration_tasmax_from_t2m(calibration_tas, settings)


def _cmip6_calibration(
    settings: CMIP6Settings,
) -> xr.DataArray:
    def create() -> Path:
        calibration = open_cmip6_calibration(settings)
        calibration.name = "tasmax_calibration"
        calibration.attrs.update(
            {
                "calibration_period": (
                    f"{settings.calibration_start} to {settings.calibration_end}"
                ),
                "grid_label": settings.grid,
                "model_id": settings.model,
            }
        )
        return write_calibration_netcdf(
            calibration,
            settings.calibration_output,
            settings=settings,
            policy=ExistingFilePolicy.OVERWRITE,
        )

    resolved = resolve_calibration(settings, create)
    if resolved is not None:
        return open_saved_tasmax_calibration(resolved, settings)
    return open_cmip6_calibration(settings)
