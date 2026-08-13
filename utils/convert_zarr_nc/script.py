"""Convert Xarray datasets between Zarr and NetCDF formats.

The functions in this module are small, explicit examples for occasional data
preparation and inspection tasks. They are not part of an index workflow and do
not expose a command-line interface.
"""

from pathlib import Path

import xarray as xr


def zarr_to_netcdf(source: Path, destination: Path) -> None:
    """Read a Zarr store and write it as a NetCDF file.

    Parameters
    ----------
    source
        Existing Zarr store to read.
    destination
        NetCDF path to write.
    """
    with xr.open_zarr(source) as dataset:
        dataset.load().to_netcdf(destination)


def netcdf_to_zarr(source: Path, destination: Path) -> None:
    """Read a NetCDF file and write it as a Zarr store.

    Parameters
    ----------
    source
        Existing NetCDF file to read.
    destination
        Zarr store path to write.
    """
    with xr.open_dataset(source) as dataset:
        dataset.load().to_zarr(destination, mode="w")
