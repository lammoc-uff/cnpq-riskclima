import math
from pathlib import Path

import numpy as np
import xarray as xr
from tqdm.auto import tqdm

from src.config.settings import LAT_BLOCK_SIZE, LON_BLOCK_SIZE
from src.io.writers import build_monthly_output_dataset, normalize_months
from src.pipeline.block_processor import process_month_block_torch
from src.pipeline.data_access import iter_spatial_blocks, open_calibration_tasmax_from_t2m, open_era5_inputs
from src.preprocessing.era5 import open_saved_tasmax_calibration


def compute_era5_monthly_xhwi_torch(
    path: str,
    calibration_path: Path | str | None = None,
    months: list[int] | tuple[int, ...] | None = None,
) -> xr.Dataset:
    months = normalize_months(months)
    print("Opening and preprocessing ERA5 inputs...")
    tas_c, hurs = open_era5_inputs(path)

    if calibration_path is None:
        print("Computing calibration tasmax from ERA5 t2m...")
        tasmax_calibration = open_calibration_tasmax_from_t2m(tas_c)
    else:
        print(f"Opening saved calibration file: {calibration_path}")
        tasmax_calibration = open_saved_tasmax_calibration(calibration_path)

    lat_values = tas_c["lat"].values
    lon_values = tas_c["lon"].values
    lat_size = tas_c.sizes["lat"]
    lon_size = tas_c.sizes["lon"]
    n_blocks = math.ceil(lat_size / LAT_BLOCK_SIZE) * math.ceil(lon_size / LON_BLOCK_SIZE)

    month_arrays = []
    month_times = []

    for month in tqdm(months, desc="Processing selected months", unit="month"):
        tqdm.write(f"Processing ERA5, calendar month {month:02d}...")
        tas_c_month = tas_c.sel(time=tas_c["time.month"] == month)
        hurs_month = hurs.sel(time=hurs["time.month"] == month)
        tasmax_calibration_month = tasmax_calibration.sel(
            calibration_time=tasmax_calibration["calibration_time.month"] == month
        )

        if tas_c_month.sizes.get("time", 0) == 0:
            tqdm.write(f"No hourly t2m data found for month {month:02d}; skipping.")
            continue
        if tasmax_calibration_month.sizes.get("calibration_time", 0) == 0:
            raise ValueError(f"No calibration tasmax data found for month {month}.")

        month_template = None
        month_time = None
        block_iter = iter_spatial_blocks(lat_size, lon_size, LAT_BLOCK_SIZE, LON_BLOCK_SIZE)

        for lat_slice, lon_slice in tqdm(
            block_iter,
            total=n_blocks,
            desc=f"Month {month:02d} spatial blocks",
            unit="block",
            leave=False,
        ):
            block_np, block_time = process_month_block_torch(
                tas_c_month=tas_c_month,
                hurs_month=hurs_month,
                tasmax_calibration_month=tasmax_calibration_month,
                lat_slice=lat_slice,
                lon_slice=lon_slice,
            )

            if month_template is None:
                month_time = block_time
                month_template = np.full((len(month_time), lat_size, lon_size), np.nan, dtype="float32")
            elif len(block_time) != len(month_time):
                raise ValueError("Inconsistent monthly time length across spatial blocks.")

            month_template[:, lat_slice, lon_slice] = block_np

        if month_template is not None:
            month_arrays.append(month_template)
            month_times.append(month_time)

    if not month_arrays:
        raise ValueError("No monthly XHWI outputs were generated for ERA5.")

    monthly_values = np.concatenate(month_arrays, axis=0)
    monthly_time = np.concatenate(month_times, axis=0)
    monthly = xr.DataArray(
        monthly_values,
        dims=("time", "lat", "lon"),
        coords={"time": monthly_time, "lat": lat_values, "lon": lon_values},
        name="xhwi_monthly_accumulated",
    ).sortby("time")

    ds = build_monthly_output_dataset(monthly)
    ds.attrs["processed_calendar_months"] = ", ".join(f"{month:02d}" for month in months)
    return ds
