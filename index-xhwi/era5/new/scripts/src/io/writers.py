from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar

from src.config.settings import (
    CALIBRATION_PERIOD,
    CDF_THRESHOLD_PERCENT,
    DATASET_ID,
    DEVICE,
    MONTHLY_OUTPUT_FILE,
    MONTHLY_PARTS_DIR,
    MONTHS_TO_RUN,
    SOURCE,
    TEMPERATURE_THRESHOLD_C,
    VARIABLE_D2M,
    VARIABLE_T2M,
)


def build_monthly_output_dataset(monthly: xr.DataArray) -> xr.Dataset:
    ds = monthly.to_dataset(name="xhwi_monthly_accumulated")
    creation_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    time_start = (
        np.datetime_as_string(ds["time"].values[0], unit="D")
        if "time" in ds.coords and ds.sizes.get("time", 0)
        else "unknown"
    )
    time_end = (
        np.datetime_as_string(ds["time"].values[-1], unit="D")
        if "time" in ds.coords and ds.sizes.get("time", 0)
        else "unknown"
    )
    lat_min = float(ds["lat"].min()) if "lat" in ds.coords else float("nan")
    lat_max = float(ds["lat"].max()) if "lat" in ds.coords else float("nan")
    lon_min = float(ds["lon"].min()) if "lon" in ds.coords else float("nan")
    lon_max = float(ds["lon"].max()) if "lon" in ds.coords else float("nan")

    if "time" in ds.coords:
        ds["time"].attrs.update({"standard_name": "time", "long_name": "Time", "axis": "T"})
    if "lat" in ds.coords:
        ds["lat"].attrs.update(
            {
                "standard_name": "latitude",
                "long_name": "Latitude",
                "units": "degrees_north",
                "axis": "Y",
            }
        )
    if "lon" in ds.coords:
        ds["lon"].attrs.update(
            {
                "standard_name": "longitude",
                "long_name": "Longitude",
                "units": "degrees_east",
                "axis": "X",
            }
        )

    ds["xhwi_monthly_accumulated"].attrs.update(
        {
            "long_name": "Monthly accumulated Extreme Heatwave Index",
            "units": "1",
            "cell_methods": "time: sum",
            "description": (
                "Monthly sum of daily XHWI products. Each daily product is the number "
                "of hours with nonzero XHWI multiplied by the daily sum of hourly XHWI."
            ),
        }
    )

    ds.attrs.update(
        {
            "title": "ERA5 - monthly accumulated Extreme Heatwave Index (XHWI)",
            "summary": (
                "Subdomain calculation of the monthly accumulated Extreme Heatwave Index "
                f"from ERA5 single-level data. Spatial domain: lat [{lat_min:.2f} deg, {lat_max:.2f} deg], "
                f"lon [{lon_min:.2f} deg, {lon_max:.2f} deg]. Temporal coverage: {time_start} to {time_end}. "
                "The index is computed from hourly 2 m temperature and relative humidity derived from 2 m dewpoint temperature. "
                "The calibration CDF is calendar-month-specific and uses daily maximum 2 m temperature derived from hourly ERA5 t2m for 1961-1990. "
                "Prepared for use in the RiskClima climate-risk index pipelines."
            ),
            "keywords": "xhwi, extreme heatwave index, ERA5, reanalysis, heatwave, climate risk, South America, RiskClima",
            "source": (
                "ERA5 hourly data on single levels from the Copernicus Climate Data Store analysis-ready cloud-optimised Zarr store. "
                "Variables used: 2 m temperature (t2m) and 2 m dewpoint temperature (d2m). "
                "Original zarr: reanalysis_era5_single_levels/sfc/geoChunked.zarr."
            ),
            "history": f"{creation_date} Computed monthly accumulated XHWI from ERA5 ARCO Zarr using xarray and a PyTorch blockwise implementation.",
            "creation_date": creation_date,
            "creator": "Marcio Cataldi <mcataldi@id.uff.br>; Livia Sancho <liviasancho@gmail.com>; Louise da Fonseca Aguiar <louisedaguiar@gmail.com>; Priscila Esposte Coutinho <priscila.esposte@gmail.com>; Vitor Luiz Victalino Galves <vitor_luiz@id.uff.br>",
            "references": "https://riskclima.com.br/",
            "code_repository": "https://github.com/lammoc-uff/cnpq-riskclima",
            "institution": "Climate System Monitoring and Modeling Laboratory (LAMMOC), Universidade Federal Fluminense (UFF), Niteroi, Brazil",
            "project": "RiskClima",
            "license": "CC-BY-4.0",
            "Conventions": "CF-1.10",
            "processing_level": "Processed data",
            "dataset_id": DATASET_ID,
            "source_id": SOURCE,
            "calibration_period": f"{CALIBRATION_PERIOD[0]} to {CALIBRATION_PERIOD[1]}",
            "calibration_method": "Separate empirical CDF for each calendar month and grid cell.",
            "compute_backend": f"PyTorch on {DEVICE.type}",
            "input_variables": f"{VARIABLE_T2M}, {VARIABLE_D2M}",
            "humidity_method": "Relative humidity calculated from 2 m dewpoint temperature and 2 m temperature.",
            "temperature_threshold": f"{TEMPERATURE_THRESHOLD_C} degC",
            "cdf_threshold": f"p{int(CDF_THRESHOLD_PERCENT)}",
            "comment": "Monthly accumulated XHWI computed from ERA5 ARCO data. Calibration is based on 1961-1990 daily maximum t2m, separately for each calendar month and grid cell.",
        }
    )
    return ds


def normalize_months(months: list[int] | tuple[int, ...] | None = None) -> list[int]:
    if months is None:
        months = MONTHS_TO_RUN
    months = [int(month) for month in months]
    invalid_months = [month for month in months if month < 1 or month > 12]
    if invalid_months:
        raise ValueError(f"Invalid months: {invalid_months}. Expected values from 1 to 12.")
    if len(months) != len(set(months)):
        raise ValueError(f"Duplicated months found: {months}")
    return sorted(months)


def months_tag(months: list[int] | tuple[int, ...] | None = None) -> str:
    months = normalize_months(months)
    if months == list(range(1, 13)):
        return "01-12"
    return "-".join(f"{month:02d}" for month in months)


def monthly_part_path(months: list[int] | tuple[int, ...] | None = None) -> Path:
    months = normalize_months(months)
    if len(months) == 1:
        filename = f"xhwi_era5_month_{months[0]:02d}.nc"
    else:
        filename = f"xhwi_era5_months_{months_tag(months)}.nc"
    return MONTHLY_PARTS_DIR / filename


def write_monthly_netcdf(
    ds: xr.Dataset,
    output_path: Path | str | None = None,
    overwrite: bool = True,
) -> Path:
    output_path = Path(output_path) if output_path is not None else MONTHLY_OUTPUT_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds = ds.sortby("time")

    if "time" in ds.indexes and not ds.indexes["time"].is_monotonic_increasing:
        raise ValueError("Output time coordinate is not monotonic increasing.")
    if "time" in ds.indexes and not ds.indexes["time"].is_unique:
        raise ValueError("Output time coordinate contains duplicated values.")

    encoding = {
        "xhwi_monthly_accumulated": {
            "zlib": True,
            "complevel": 4,
            "_FillValue": np.float32(np.nan),
            "dtype": "float32",
        }
    }

    if output_path.exists():
        if overwrite:
            output_path.unlink()
        else:
            raise FileExistsError(f"File already exists: {output_path}")

    with ProgressBar():
        ds.to_netcdf(output_path, engine="netcdf4", encoding=encoding)
    return output_path


def concat_monthly_netcdfs(
    input_paths: list[Path | str] | None = None,
    output_path: Path | str | None = None,
    overwrite: bool = True,
) -> Path:
    if input_paths is None:
        input_paths = sorted(MONTHLY_PARTS_DIR.glob("xhwi_era5_month*.nc"))
    input_paths = [Path(path) for path in input_paths]
    if not input_paths:
        raise ValueError(f"No monthly part files found in {MONTHLY_PARTS_DIR}")

    print("Files to concatenate:")
    for path in input_paths:
        print(path)

    ds_raw = xr.open_mfdataset(input_paths, combine="nested", concat_dim="time", engine="netcdf4")
    ds_raw = ds_raw.sortby("time")

    if "time" in ds_raw.indexes and not ds_raw.indexes["time"].is_unique:
        duplicated_times = ds_raw.indexes["time"][ds_raw.indexes["time"].duplicated()]
        raise ValueError(
            "Duplicated time values found during concatenation. "
            f"First duplicated values: {duplicated_times[:10]}"
        )

    monthly = ds_raw["xhwi_monthly_accumulated"]
    ds_final = build_monthly_output_dataset(monthly)
    ds_final.attrs["source_monthly_part_files"] = "; ".join(str(path) for path in input_paths)
    output_path = Path(output_path) if output_path is not None else MONTHLY_OUTPUT_FILE
    written_path = write_monthly_netcdf(ds_final, output_path=output_path, overwrite=overwrite)
    ds_raw.close()
    return written_path
