# Zarr and NetCDF conversion

This directory contains `script.py`, a small reference utility for converting datasets between Zarr and NetCDF.

The utility is kept here as documentation and practical support. Some RiskClima workflows receive Zarr stores while other steps inspect or exchange NetCDF files, so the conversion examples are useful when preparing inputs or checking intermediate data.

## Functions

`script.py` provides:

- `zarr_to_netcdf(source, target)` to read a Zarr store and write a NetCDF file;
- `netcdf_to_zarr(source, target)` to read a NetCDF file and write a Zarr store.

The module has no command-line interface. Import the function needed by a notebook or an auxiliary Python script and provide the source and destination paths explicitly.

## Example

```python
from pathlib import Path

from script import netcdf_to_zarr, zarr_to_netcdf

zarr_to_netcdf(
    Path("input/data.zarr"),
    Path("output/data.nc"),
)
netcdf_to_zarr(
    Path("input/data.nc"),
    Path("output/data.zarr"),
)
```

The destination is replaced by the underlying Xarray writer when it already exists. Confirm the source schema, coordinates, variables, units, and available disk space before converting a large climate dataset.
