import xarray as xr
from dask.diagnostics import ProgressBar


def to_zarr(ds: xr.Dataset, path, mode: str = "w", consolidated: bool = True):
    """
    Write an xarray Dataset to a Zarr store with a Dask progress bar.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to be written.
    path : str or Path
        Destination path for the Zarr store.
    mode : str, optional
        Write mode. Default is "w" (overwrite). Use "a" to append.
    consolidated : bool, optional
        Whether to write a consolidated metadata tree. Default is True.

    Notes
    -----
    This wrapper standardizes writing to Zarr with visual feedback
    and ensures Dask operations are properly executed.
    """
    with ProgressBar():
        ds.to_zarr(str(path), mode=mode, consolidated=consolidated)
