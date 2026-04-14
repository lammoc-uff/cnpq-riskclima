# src/preprocessing.py

import pandas as pd
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex


def preprocess_time(ds: xr.Dataset, calendar: str = "proleptic_gregorian") -> xr.Dataset:
    """
    Standardize and clean the time coordinate of a CMIP6 dataset.

    Steps:
      - Sort by time if needed.
      - Convert CFTimeIndex to a standard calendar.
      - Drop duplicate timestamps.
      - If SSP experiment starts at 2015, crop to 2015–2050.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing a 'time' coordinate.
    calendar : str, optional
        Target calendar format. Default = 'proleptic_gregorian'.

    Returns
    -------
    xr.Dataset
        Dataset with cleaned time coordinate.
    """
    if "time" not in ds.coords:
        return ds

    # Sort by time if not monotonic
    if not ds.indexes["time"].is_monotonic_increasing:
        ds = ds.sortby("time")

    # Convert and deduplicate
    if isinstance(ds.indexes["time"], CFTimeIndex):
        ds = ds.convert_calendar(calendar, align_on="year")
        
        try:
            ds["time"] = ds.indexes["time"].to_datetimeindex()
        except Exception:
            pass
        
        time_index = pd.Index(ds.indexes["time"])
        mask = ~time_index.duplicated()
        if mask.sum() < len(mask):
            ds = ds.isel(time=mask)
    else:
        time_index = ds.indexes["time"]
        mask = ~time_index.duplicated()
        if mask.sum() < len(mask):
            ds = ds.isel(time=mask)

    # Crop SSP experiments (2015–2050 window)
    try:
        t0_year = ds.indexes["time"][0].year
        if t0_year == 2015:
            ds = ds.sel(time=slice("2015-01-01", "2050-12-31"))
    except Exception as e:
        print(f"⚠️ Failed to interpret time start year: {e}")

    return ds


def adjust_coordinates_flex(ds: xr.Dataset) -> xr.Dataset:
    """
    Normalize latitude/longitude coordinates.

    - Longitude → [-180, 180] if originally [0, 360].
    - Latitude sorted if 1D.
    - Skip sorting for curvilinear grids (2D).
    - Skip adjustment entirely for SST datasets ('tos').

    Parameters
    ----------
    ds : xr.Dataset

    Returns
    -------
    xr.Dataset
    """
    if "tos" in ds.data_vars:
        return ds

    lon_name, lat_name = None, None
    for coord in ds.coords:
        if coord in ("lon", "longitude"):
            lon_name = coord
        elif coord in ("lat", "latitude"):
            lat_name = coord

    # Adjust longitude
    if lon_name:
        lon = ds[lon_name]
        if lon.ndim == 1:
            lon_new = ((lon + 180) % 360) - 180
            ds = ds.assign_coords({lon_name: lon_new})
            ds = ds.sortby(lon_name)
        elif lon.ndim == 2:
            lon_new = ((lon + 180) % 360) - 180
            ds = ds.assign_coords({lon_name: (lon.dims, lon_new)})

    # Sort latitude if 1D
    if lat_name:
        lat = ds[lat_name]
        if lat.ndim == 1:
            ds = ds.sortby(lat_name)

    return ds


def crop_to_brazil(ds: xr.Dataset) -> xr.Dataset:
    """
    Crop dataset to Brazil / extended South America.

    Applies only if lat/lon are 1D. Skip for curvilinear grids (2D).
    Skip cropping for 'tos'.

    Bounds:
      lat: -70 → 20
      lon: -120 → -30

    Parameters
    ----------
    ds : xr.Dataset

    Returns
    -------
    xr.Dataset
    """
    if "tos" in ds.data_vars:
        return ds

    lat_name, lon_name = None, None
    for coord in ds.coords:
        if coord.lower() in ("lat", "latitude"):
            lat_name = coord
        elif coord.lower() in ("lon", "longitude"):
            lon_name = coord

    if (
        lat_name is None or lon_name is None
        or ds[lat_name].ndim != 1
        or ds[lon_name].ndim != 1
    ):
        return ds

    return ds.sel(
        **{
            lat_name: slice(-70, 20),
            lon_name: slice(-120, -30),
        }
    )