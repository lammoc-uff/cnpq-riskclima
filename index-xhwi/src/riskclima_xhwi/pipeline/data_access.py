import operator
from collections.abc import Iterator

import xarray as xr

from riskclima_xhwi.config.settings import (
    CMIP6Scenario,
    CMIP6Settings,
    ERA5LandSettings,
    ERA5Settings,
)
from riskclima_xhwi.features.humidity import (
    dewpoint_to_relative_humidity,
    specific_to_relative_humidity_standard_pressure,
)
from riskclima_xhwi.preprocessing.cmip6 import interpolate_to_hourly, open_clean_cmip6_zarr
from riskclima_xhwi.preprocessing.era5 import kelvin_to_celsius, open_era5_zarr


def get_era5_variable(ds: xr.Dataset, long_name: str, short_name: str) -> xr.DataArray:
    """Get an ERA5 variable by an ARCO or short name.

    Parameters
    ----------
    ds
        ERA5 dataset.
    long_name
        Preferred ARCO variable name.
    short_name
        ERA5 short variable name.

    Returns
    -------
    xarray.DataArray
        Requested variable.

    Raises
    ------
    KeyError
        If neither name exists.
    """
    if long_name in ds:
        return ds[long_name]
    if short_name in ds:
        return ds[short_name]
    raise KeyError(f"Variable not found. Tried: {long_name}, {short_name}")


def open_era5_inputs(path: str, settings: ERA5Settings) -> tuple[xr.DataArray, xr.DataArray]:
    """Open and preprocess ERA5 temperature and humidity inputs."""
    ds = open_era5_zarr(
        path,
        chunks=settings.zarr_chunks,
        consolidated=settings.zarr_consolidated,
        timeout_seconds=settings.request_timeout_seconds,
    ).sel(
        time=slice(*settings.application_period),
        lat=settings.latitude_slice,
        lon=settings.longitude_slice,
    )
    t2m = get_era5_variable(ds, settings.variable_t2m, settings.variable_t2m_alias)
    d2m = get_era5_variable(ds, settings.variable_humidity, settings.variable_humidity_alias)
    return kelvin_to_celsius(t2m), dewpoint_to_relative_humidity(d2m=d2m, t2m=t2m)


def open_era5_land_inputs(
    path: str, settings: ERA5LandSettings
) -> tuple[xr.DataArray, xr.DataArray]:
    """Open ERA5-Land inputs and select its explicit application period.

    Parameters
    ----------
    path
        ERA5-Land temperature ARCO Zarr URL. The dewpoint URL comes from settings.
    settings
        Domain, variables, and application period.

    Returns
    -------
    tuple of xarray.DataArray
        Temperature in degrees Celsius and relative humidity in percent.
    """
    period = slice(*settings.application_period)
    temperature_ds = open_era5_zarr(
        path,
        chunks=settings.zarr_chunks,
        consolidated=settings.zarr_consolidated,
        timeout_seconds=settings.request_timeout_seconds,
    ).sel(time=period, lat=settings.latitude_slice, lon=settings.longitude_slice)
    dewpoint_ds = open_era5_zarr(
        settings.dewpoint_zarr_url,
        chunks=settings.zarr_chunks,
        consolidated=settings.zarr_consolidated,
        timeout_seconds=settings.request_timeout_seconds,
    ).sel(time=period, lat=settings.latitude_slice, lon=settings.longitude_slice)
    t2m = get_era5_variable(temperature_ds, settings.variable_t2m, settings.variable_t2m_alias)
    d2m = get_era5_variable(
        dewpoint_ds, settings.variable_humidity, settings.variable_humidity_alias
    )
    try:
        t2m, d2m = xr.align(t2m, d2m, join="exact")
    except ValueError as error:
        raise ValueError(
            "ERA5-Land temperature and dewpoint coordinates must match exactly."
        ) from error
    return kelvin_to_celsius(t2m), dewpoint_to_relative_humidity(d2m=d2m, t2m=t2m)


def open_t2m_calibration_inputs(path: str, settings: ERA5Settings) -> xr.DataArray:
    """Open and preprocess ERA5 temperature for calibration."""
    ds = open_era5_zarr(
        path,
        chunks=settings.zarr_chunks,
        consolidated=settings.zarr_consolidated,
        timeout_seconds=settings.request_timeout_seconds,
    ).sel(lat=settings.latitude_slice, lon=settings.longitude_slice)
    t2m = get_era5_variable(ds, settings.variable_t2m, settings.variable_t2m_alias)
    return kelvin_to_celsius(t2m)


def open_calibration_tasmax_from_t2m(
    t2m_c: xr.DataArray,
    settings: ERA5Settings,
) -> xr.DataArray:
    """Compute calibration daily maxima from hourly ERA5 temperature.

    Parameters
    ----------
    t2m_c
        Hourly two-meter temperature in degrees Celsius.
    settings
        Configuration defining the calibration period.

    Returns
    -------
    xarray.DataArray
        Daily maximum temperature with a calibration_time dimension.

    Raises
    ------
    ValueError
        If the calibration period contains no data.
    """
    t2m_cal = t2m_c.sel(time=slice(*settings.calibration_period))
    if t2m_cal.sizes.get("time", 0) == 0:
        raise ValueError(f"No t2m data found for calibration period {settings.calibration_period}.")
    coarsened = t2m_cal.coarsen(time=24, boundary="trim")
    tasmax = operator.methodcaller("max")(coarsened)
    daily_time = t2m_cal["time"].isel(time=slice(0, None, 24))
    daily_time = daily_time.isel(time=slice(0, tasmax.sizes["time"]))
    tasmax = tasmax.assign_coords(time=daily_time)
    return tasmax.rename({"time": "calibration_time"})


def open_cmip6_calibration(settings: CMIP6Settings) -> xr.DataArray:
    """Open native historical daily tasmax for CMIP6 calibration.

    Parameters
    ----------
    settings
        CMIP6 identity, layout, chunks, and calibration period.

    Returns
    -------
    xarray.DataArray
        Native daily maximum temperature with a calibration-time dimension.
    """
    chunks = {
        "time": settings.calibration_time_chunk,
        "lat": settings.spatial_chunk,
        "lon": settings.spatial_chunk,
    }
    ds = open_clean_cmip6_zarr(
        settings.calibration_source,
        chunks=chunks,
        consolidated=settings.zarr_consolidated,
    )
    tasmax = kelvin_to_celsius(ds[settings.variable_tasmax]).sel(
        time=slice(*settings.calibration_period)
    )
    if tasmax.sizes.get("time", 0) == 0:
        raise ValueError(
            f"No tasmax data found for calibration period {settings.calibration_period}."
        )
    return tasmax.rename({"time": "calibration_time"})


def open_cmip6_hourly_inputs(
    scenario: CMIP6Scenario,
    settings: CMIP6Settings,
) -> tuple[xr.DataArray, xr.DataArray]:
    """Open CMIP6 tas and huss, interpolate both hourly, then derive humidity.

    Parameters
    ----------
    scenario
        CMIP6 experiment identifier.
    settings
        CMIP6 identity, layout, chunks, and pressure configuration.

    Returns
    -------
    tuple of xarray.DataArray
        Hourly temperature in degrees Celsius and relative humidity in percent.
    """
    chunks = {
        "time": settings.time_chunk,
        "lat": settings.spatial_chunk,
        "lon": settings.spatial_chunk,
    }
    tas_ds = interpolate_to_hourly(
        open_clean_cmip6_zarr(
            settings.scenario_input(scenario, settings.variable_tas),
            chunks=chunks,
            consolidated=settings.zarr_consolidated,
        ),
        settings.interpolation_frequency,
        settings.interpolation_method,
    )
    huss_ds = interpolate_to_hourly(
        open_clean_cmip6_zarr(
            settings.scenario_input(scenario, settings.variable_huss),
            chunks=chunks,
            consolidated=settings.zarr_consolidated,
        ),
        settings.interpolation_frequency,
        settings.interpolation_method,
    )
    tas = tas_ds[settings.variable_tas].sel(time=slice(*settings.application_period))
    huss = huss_ds[settings.variable_huss].sel(time=slice(*settings.application_period))
    try:
        tas, huss = xr.align(tas, huss, join="exact")
    except ValueError as error:
        raise ValueError("CMIP6 tas and huss coordinates must match exactly.") from error
    return kelvin_to_celsius(tas), specific_to_relative_humidity_standard_pressure(
        huss, tas, pressure_pa=settings.standard_pressure_pa
    )


def iter_spatial_blocks(
    lat_size: int,
    lon_size: int,
    lat_block: int,
    lon_block: int,
) -> Iterator[tuple[slice, slice]]:
    """Yield bounded latitude and longitude index blocks."""
    for lat_start in range(0, lat_size, lat_block):
        lat_stop = min(lat_start + lat_block, lat_size)
        for lon_start in range(0, lon_size, lon_block):
            lon_stop = min(lon_start + lon_block, lon_size)
            yield slice(lat_start, lat_stop), slice(lon_start, lon_stop)
