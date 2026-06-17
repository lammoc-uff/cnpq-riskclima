def clear_chunks_encoding(ds):
    """
    Remove 'chunks' from variable encodings in an xarray Dataset.

    This avoids compatibility issues when rechunking or saving to Zarr,
    since different input files (e.g., NetCDF, Zarr) may contain inconsistent
    chunk encodings that prevent optimized writes.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset whose variable encodings will be sanitized.

    Returns
    -------
    xarray.Dataset
        The same dataset instance with cleaned encodings.
    """
    for var in ds.variables:
        if "chunks" in ds[var].encoding:
            del ds[var].encoding["chunks"]
    return ds
