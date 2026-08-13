from collections.abc import Mapping
from importlib import import_module

import xarray as xr
from shapely.geometry.base import BaseGeometry

_RIOXARRAY_MODULE = import_module("rioxarray")


def gridslice(
    dataset: xr.DataArray,
    contour: Mapping[str, BaseGeometry],
    crs: str = "epsg:4989",
    xdim: str = "longitude",
    ydim: str = "latitude",
) -> xr.DataArray:
    """Clip a gridded field to a polygon and normalize its longitude axis.

    Parameters
    ----------
    dataset
        Field containing the spatial dimensions to clip.
    contour
        Mapping whose ``geometry`` value is the clipping polygon.
    crs
        Coordinate reference system shared by the grid and polygon.
    xdim
        Name of the longitude dimension.
    ydim
        Name of the latitude dimension.

    Returns
    -------
    xarray.DataArray
        Clipped field with longitudes normalized to the ``[-180, 180)`` range.
    """
    dataset = dataset.rio.set_spatial_dims(x_dim=xdim, y_dim=ydim)
    dataset = dataset.rio.write_crs(crs)
    sliced = dataset.rio.clip([contour["geometry"]], crs)
    new_xdim = ((sliced[xdim] + 180) % 360) - 180
    sliced = sliced.assign_coords({xdim: new_xdim}).sortby(xdim)
    return sliced
