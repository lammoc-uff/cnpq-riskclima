import os
from pathlib import Path

import xarray as xr


def standardize_era5_dims(ds: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
    """Rename ERA5 coordinates to lat/lon/time if needed and sort dimensions."""
    rename = {}

    if "latitude" in ds.dims or "latitude" in ds.coords:
        rename["latitude"] = "lat"
    if "longitude" in ds.dims or "longitude" in ds.coords:
        rename["longitude"] = "lon"
    if "valid_time" in ds.dims or "valid_time" in ds.coords:
        rename["valid_time"] = "time"

    if rename:
        ds = ds.rename(rename)

    for dim in ["time", "calibration_time", "lat", "lon"]:
        if dim in ds.dims:
            ds = ds.sortby(dim)

    return ds


def get_cdsapi_key() -> str:
    key = os.getenv("CDSAPI_KEY")
    if key and key.strip():
        return key.strip()

    try:
        from google.colab import userdata

        key = userdata.get("CDSAPI_KEY")
        if key and key.strip():
            return key.strip()
    except Exception:
        pass

    cdsapirc = Path.home() / ".cdsapirc"
    if cdsapirc.exists():
        for line in cdsapirc.read_text().splitlines():
            if line.strip().startswith("key:"):
                key = line.split("key:", 1)[1].strip()
                if key:
                    return key

    raise ValueError(
        "CDS API key not found. Set CDSAPI_KEY, add it to Colab Secrets, or create ~/.cdsapirc."
    )


def open_era5_zarr(path: str, chunks="auto") -> xr.Dataset:
    cdsapi_key = get_cdsapi_key()
    ds = xr.open_zarr(
        path,
        consolidated=True,
        chunks=chunks,
        storage_options={
            "timeout": 600, 
            "headers": {
                "Authorization": f"Bearer {cdsapi_key}"
                }
                },
    )
    return standardize_era5_dims(ds)


def open_saved_tasmax_calibration(calibration_path: Path | str) -> xr.DataArray:
    da = xr.open_dataarray(calibration_path)
    da = standardize_era5_dims(da)

    if "time" in da.dims:
        da = da.rename({"time": "calibration_time"})

    if "calibration_time" not in da.dims:
        raise ValueError("Saved calibration file must have 'calibration_time' dimension.")
    if "lat" not in da.dims or "lon" not in da.dims:
        raise ValueError("Saved calibration file must have 'lat' and 'lon' dimensions.")

    return da


def kelvin_to_celsius(da: xr.DataArray) -> xr.DataArray:
    units = str(da.attrs.get("units", "")).lower()
    out = da - 273.15 if units in {"k", "kelvin"} else da
    out.attrs = da.attrs.copy()
    out.attrs["units"] = "degC"
    return out
