from collections.abc import Mapping
from pathlib import Path

import xarray as xr

from riskclima_xhwi.config.settings import InterpolationMethod


def clean_cmip6_dims(ds: xr.Dataset) -> xr.Dataset:
    """Keep only variables defined on time, latitude, and longitude.

    Parameters
    ----------
    ds
        Raw CMIP6 dataset.

    Returns
    -------
    xarray.Dataset
        Dataset stripped of bounds and auxiliary dimensions and sorted by dimension.
    """
    keep_dims = {"time", "lat", "lon"}
    variables = [name for name in ds.data_vars if set(ds[name].dims).issubset(keep_dims)]
    cleaned = ds[variables]
    auxiliary_coords = [coord for coord in cleaned.coords if coord not in keep_dims]
    cleaned = cleaned.drop_vars(auxiliary_coords, errors="ignore")
    for dim in ("time", "lat", "lon"):
        if dim in cleaned.dims:
            cleaned = cleaned.sortby(dim)
    return cleaned


def open_clean_cmip6_zarr(
    path: Path | str,
    chunks: Mapping[str, int] | None = None,
    *,
    consolidated: bool,
) -> xr.Dataset:
    """Open a local CMIP6 Zarr store and clean its dimensions.

    Parameters
    ----------
    path
        Local Zarr store path.
    chunks
        Xarray chunk sizes.
    consolidated
        Whether the store uses consolidated Zarr metadata.

    Returns
    -------
    xarray.Dataset
        Cleaned CMIP6 dataset.
    """
    return clean_cmip6_dims(xr.open_zarr(path, chunks=chunks, consolidated=consolidated))


def interpolate_to_hourly(
    ds: xr.Dataset, frequency: str, method: InterpolationMethod
) -> xr.Dataset:
    """Interpolate a sorted CMIP6 dataset to the configured resolution.

    Parameters
    ----------
    ds
        Dataset with a time coordinate.
    frequency
        Xarray resampling frequency.
    method
        Xarray interpolation method.

    Returns
    -------
    xarray.Dataset
        Dataset interpolated at the configured frequency and method.
    """
    interpolation = "linear" if method is InterpolationMethod.LINEAR else "nearest"
    return ds.sortby("time").resample(time=frequency).interpolate(interpolation)
