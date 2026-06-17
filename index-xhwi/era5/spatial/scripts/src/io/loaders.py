import xarray as xr
from src.config.settings import CALIB_PATH, ZARR_R, ZARR_T2M, FIX_LONGITUDE_TO_180


def _fix_longitude(ds: xr.Dataset) -> xr.Dataset:
    """
    Normalize longitudes from 0–360 to -180–180 if configured.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with normalized and sorted longitude if applicable.
    """
    if FIX_LONGITUDE_TO_180 and "longitude" in ds.coords:
        ds = ds.assign_coords(longitude=((ds.longitude + 180) % 360) - 180)
        ds = ds.sortby("longitude")
    return ds


def open_calibration() -> xr.Dataset:
    """
    Open the calibration dataset (e.g., 1960–1990 temperature maxima).

    Returns
    -------
    xarray.Dataset
        Calibration dataset with fixed longitude if applicable.
    """
    ds = xr.open_dataset(str(CALIB_PATH))
    return _fix_longitude(ds)


def open_validation_pair() -> xr.Dataset:
    """
    Open humidity (r) and temperature (t2m) validation Zarr datasets,
    attach humidity into temperature dataset, and normalize longitude.

    Returns
    -------
    xarray.Dataset
        Combined dataset with variables:
        - 't2m' : 2-meter temperature
        - 'r'   : relative humidity
    """
    ds_r = xr.open_zarr(str(ZARR_R))
    ds_t = xr.open_zarr(str(ZARR_T2M))

    # Attach humidity into main dataset
    ds_t["r"] = ds_r["r"]

    return _fix_longitude(ds_t)
