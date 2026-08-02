import xarray as xr

from src.config.settings import CALIBRATION_PERIOD, LAT_SLICE, LON_SLICE, VARIABLE_D2M, VARIABLE_T2M
from src.features.humidity import dewpoint_to_relative_humidity
from src.preprocessing.era5 import kelvin_to_celsius, open_era5_zarr


def get_era5_variable(ds: xr.Dataset, long_name: str, short_name: str) -> xr.DataArray:
    """Get ERA5 variable accepting either ARCO long names or ERA5 short names."""
    if long_name in ds:
        return ds[long_name]
    if short_name in ds:
        return ds[short_name]
    raise KeyError(f"Variable not found. Tried: {long_name}, {short_name}")


def open_era5_inputs(path: str) -> tuple[xr.DataArray, xr.DataArray]:
    ds = open_era5_zarr(path, chunks="auto")
    ds = ds.sel(lat=LAT_SLICE, lon=LON_SLICE)
    t2m = get_era5_variable(ds, VARIABLE_T2M, "t2m")
    d2m = get_era5_variable(ds, VARIABLE_D2M, "d2m")
    t2m_c = kelvin_to_celsius(t2m)
    hurs = dewpoint_to_relative_humidity(d2m=d2m, t2m=t2m)
    return t2m_c, hurs


def open_t2mcalib_inputs(path: str) -> xr.DataArray:
    ds = open_era5_zarr(path, chunks="auto")
    ds = ds.sel(lat=LAT_SLICE, lon=LON_SLICE)
    t2m = get_era5_variable(ds, VARIABLE_T2M, "t2m")
    return kelvin_to_celsius(t2m)


def open_calibration_tasmax_from_t2m(t2m_c: xr.DataArray) -> xr.DataArray:
    """Compute daily maximum 2 m temperature from hourly ERA5 t2m for calibration."""
    t2m_cal = t2m_c.sel(time=slice(*CALIBRATION_PERIOD))
    if t2m_cal.sizes.get("time", 0) == 0:
        raise ValueError(f"No t2m data found for calibration period {CALIBRATION_PERIOD}.")

    tasmax = t2m_cal.coarsen(time=24, boundary="trim").max()
    daily_time = t2m_cal["time"].isel(time=slice(0, None, 24))
    daily_time = daily_time.isel(time=slice(0, tasmax.sizes["time"]))
    tasmax = tasmax.assign_coords(time=daily_time)
    return tasmax.rename({"time": "calibration_time"})


def iter_spatial_blocks(lat_size: int, lon_size: int, lat_block: int, lon_block: int):
    for lat_start in range(0, lat_size, lat_block):
        lat_stop = min(lat_start + lat_block, lat_size)
        for lon_start in range(0, lon_size, lon_block):
            lon_stop = min(lon_start + lon_block, lon_size)
            yield slice(lat_start, lat_stop), slice(lon_start, lon_stop)
