from collections.abc import Mapping
from pathlib import Path

import xarray as xr

from riskclima_xhwi.config.settings import CDSCredentials, XHWISettings


def standardize_era5_dims(ds: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
    """Standardize and sort ERA5 dimensions.

    Parameters
    ----------
    ds
        ERA5 dataset or data array.

    Returns
    -------
    xarray.Dataset or xarray.DataArray
        Object using lat, lon, and time coordinate names when available.
    """
    rename: dict[str, str] = {}
    if "latitude" in ds.dims or "latitude" in ds.coords:
        rename["latitude"] = "lat"
    if "longitude" in ds.dims or "longitude" in ds.coords:
        rename["longitude"] = "lon"
    if "valid_time" in ds.dims or "valid_time" in ds.coords:
        rename["valid_time"] = "time"
    if rename:
        ds = ds.rename(rename)
    for dim in ("time", "calibration_time", "lat", "lon"):
        if dim in ds.dims:
            ds = ds.sortby(dim)
    return ds


def get_cdsapi_key(credentials: CDSCredentials | None = None) -> str:
    """Load the CDS API key from ``.env``, the environment, or user configuration.

    Parameters
    ----------
    credentials
        Explicit credentials configuration. When omitted, values are loaded from
        ``CDSAPI_KEY`` in the process environment or the current directory's ``.env``.

    Returns
    -------
    str
        CDS API key.

    Raises
    ------
    ValueError
        If no key is configured.
    """
    configured = credentials or CDSCredentials()
    configured_key = configured.cdsapi_key
    if configured_key is not None and configured_key.get_secret_value().strip():
        return configured_key.get_secret_value().strip()
    cdsapirc = (
        Path(configured.cdsapi_config_file).expanduser() if configured.cdsapi_config_file else None
    )
    if cdsapirc is not None and cdsapirc.exists():
        for line in cdsapirc.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("key:"):
                key = line.split("key:", 1)[1].strip()
                if key:
                    return key
    raise ValueError(
        "CDS API key not found. Set CDSAPI_KEY in .env or the process environment, "
        "or set CDSAPI_CONFIG_FILE to a credentials file."
    )


def open_era5_zarr(
    path: str,
    chunks: str | Mapping[str, int] | None,
    *,
    consolidated: bool,
    timeout_seconds: float,
) -> xr.Dataset:
    """Open the authenticated ERA5 ARCO Zarr dataset.

    Parameters
    ----------
    path
        Zarr store URL.
    chunks
        Xarray chunk configuration.
    consolidated
        Whether the store uses consolidated Zarr metadata.
    timeout_seconds
        HTTP request timeout.

    Returns
    -------
    xarray.Dataset
        Standardized ERA5 dataset.
    """
    ds = xr.open_zarr(
        path,
        consolidated=consolidated,
        chunks=chunks,
        storage_options={
            "timeout": timeout_seconds,
            "headers": {"Authorization": f"Bearer {get_cdsapi_key()}"},
        },
    )
    standardized = standardize_era5_dims(ds)
    if not isinstance(standardized, xr.Dataset):
        raise TypeError("Expected an ERA5 dataset.")
    return standardized


def open_saved_tasmax_calibration(
    calibration_path: Path | str, settings: XHWISettings
) -> xr.DataArray:
    """Open and validate a saved maximum-temperature calibration.

    Parameters
    ----------
    calibration_path
        NetCDF calibration path.
    settings
        NetCDF engine configuration.

    Returns
    -------
    xarray.DataArray
        Calibration with calibration_time, lat, and lon dimensions.

    Raises
    ------
    ValueError
        If required dimensions are absent.
    """
    da = standardize_era5_dims(xr.open_dataarray(calibration_path, engine=settings.netcdf_engine))
    if not isinstance(da, xr.DataArray):
        raise TypeError("Expected a calibration data array.")
    if "time" in da.dims:
        da = da.rename({"time": "calibration_time"})
    if "calibration_time" not in da.dims:
        raise ValueError("Saved calibration file must have 'calibration_time' dimension.")
    if "lat" not in da.dims or "lon" not in da.dims:
        raise ValueError("Saved calibration file must have 'lat' and 'lon' dimensions.")
    return da


def kelvin_to_celsius(da: xr.DataArray) -> xr.DataArray:
    """Convert a data array from kelvin to degrees Celsius when needed.

    Parameters
    ----------
    da
        Temperature data array.

    Returns
    -------
    xarray.DataArray
        Temperature represented in degrees Celsius.
    """
    units = str(da.attrs.get("units", "")).lower()
    out = da - 273.15 if units in {"k", "kelvin"} else da
    out.attrs = da.attrs.copy()
    out.attrs["units"] = "degC"
    return out
