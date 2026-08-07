"""
Calculate daily atmospheric blocking series from ERA5 data.

The script downloads the required pressure-level fields, computes relative
vorticity at 850 and 500 hPa, calculates 500 hPa geopotential anomalies,
applies the blocking persistence criterion, and saves daily indicators by region.
"""
import gc
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path
from typing import Dict, List, Tuple

import cdsapi
import metpy.calc as mpcalc
import numpy as np
import pandas as pd
import xarray as xr

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuration
START_YEAR = 1960
END_YEAR = 2025

PERSISTENCE_DAYS = 3

CLIM_PERIOD = "90_20"

DATA_DIR = Path("era5_data")
OUTPUT_DIR = Path("historical_output") / CLIM_PERIOD

FILE_CLIM_GZ = Path(f"climatology_data/clima_gz_{CLIM_PERIOD}.nc")
FILE_ERA5 = DATA_DIR / "era5_uv_gz_historical.nc"

# Limit parallel downloads to five workers
N_WORKERS = min(5, max(1, cpu_count() // 2))

# Geographic areas
AREAS: Dict[str, Dict[str, float]] = {
    "total":    {"lat_min": -25.0, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -40.0},
    "north":    {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -40.0},
    "north_h1": {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -50.0},
    "north_h2": {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -50.0, "lon_max": -40.0},
    "south":      {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -60.0, "lon_max": -40.0},
    "south_h1":   {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -60.0, "lon_max": -50.0},
    "south_h2":   {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -50.0, "lon_max": -40.0},
}


# Download ERA5 data

def _worker_download(args: Tuple[int, Path]) -> Tuple[int, Path]:
    """Download ERA5 pressure-level fields for one year."""
    year, data_dir = args
    file = data_dir / f"_era5_{year}.nc"
    if file.exists():
        log.info(f"  {year}: file already exists, skipping download.")
        return year, file

    c = cdsapi.Client(quiet=True)
    c.retrieve(
        "reanalysis-era5-pressure-levels",
        {
            "product_type": ["reanalysis"],
            "variable": ["u_component_of_wind", "v_component_of_wind", "geopotential"],
            "pressure_level": ["500", "850"],
            "year":  [str(year)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "day":   [f"{d:02d}" for d in range(1, 32)],
            "time":  ["12:00"],
            "area":  [10, -70, -35, -30],   # [N, W, S, E]
            "data_format": "netcdf",
            "download_format": "unarchived",
        },
        str(file),
    )
    return year, file


def download_era5() -> None:
    """Download yearly ERA5 files in parallel and concatenate them."""
    if FILE_ERA5.exists():
        log.info(f"Consolidated ERA5 already exists: {FILE_ERA5}")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    years = list(range(START_YEAR, END_YEAR + 1))
    log.info(f"Downloading ERA5 {START_YEAR}–{END_YEAR} with {N_WORKERS} workers...")

    results: Dict[int, Path] = {}
    with ProcessPoolExecutor(max_workers=N_WORKERS) as ex:
        futures = {ex.submit(_worker_download, (y, DATA_DIR)): y for y in years}
        for f in as_completed(futures):
            year_ret, path = f.result()
            results[year_ret] = path
            log.info(f"  {year_ret} completed → {path.name}")

    files = [results[y] for y in years]
    log.info("Concatenating annual files...")
    xr.open_mfdataset(files, combine="by_coords").to_netcdf(FILE_ERA5)
    log.info(f"Consolidated ERA5 saved: {FILE_ERA5}")


# Regional means

def crop_area_mean(da: xr.DataArray, area: Dict[str, float]) -> xr.DataArray:
    """Crop to geographic bounds and return the spatial-mean time series."""
    lat_ok = (da.latitude >= area["lat_min"]) & (da.latitude <= area["lat_max"])
    lon_ok = (da.longitude >= area["lon_min"]) & (da.longitude <= area["lon_max"])
    return da.where(lat_ok & lon_ok, drop=True).mean(dim=["latitude", "longitude"])


# Process one year

def process_year(
    ds_full: xr.Dataset,
    year: int,
    clim_np: np.ndarray,   # shape (12, lat, lon)
) -> Dict[str, Dict[str, np.ndarray]]:
    """Compute yearly blocking predictors and spatial means for each region."""
    time_coord = "valid_time" if "valid_time" in ds_full.dims else "time"
    year_mask = ds_full[time_coord].dt.year == year
    ds_year = ds_full.isel({time_coord: year_mask}).load()  # load only the selected year

    n_days = ds_year.sizes[time_coord]
    if n_days == 0:
        log.warning(f"  [{year}] no timesteps found, skipping.")
        return {}

    timestamps = pd.to_datetime(ds_year[time_coord].values).normalize()

    # Compute 850 hPa vorticity
    log.info(f"  [{year}] ({n_days} days) vorticity 850 hPa...")
    ds_850 = ds_year.sel(pressure_level=850).metpy.parse_cf()
    u850 = ds_850["u"].metpy.quantify()
    v850 = ds_850["v"].metpy.quantify()
    vort850_3d = mpcalc.vorticity(u850, v850).metpy.dequantify()
    if "metpy_crs" in vort850_3d.coords:
        vort850_3d = vort850_3d.drop_vars("metpy_crs")
    if "valid_time" in vort850_3d.dims:
        vort850_3d = vort850_3d.rename({"valid_time": "time"})
    del u850, v850, ds_850

    # Compute 500 hPa vorticity
    log.info(f"  [{year}] vorticity 500 hPa...")
    ds_500 = ds_year.sel(pressure_level=500).metpy.parse_cf()
    u500 = ds_500["u"].metpy.quantify()
    v500 = ds_500["v"].metpy.quantify()
    vort500_3d = mpcalc.vorticity(u500, v500).metpy.dequantify()
    if "metpy_crs" in vort500_3d.coords:
        vort500_3d = vort500_3d.drop_vars("metpy_crs")
    if "valid_time" in vort500_3d.dims:
        vort500_3d = vort500_3d.rename({"valid_time": "time"})
    del u500, v500, ds_500

    # Compute the 500 hPa geopotential anomaly
    log.info(f"  [{year}] geopotential anomaly 500 hPa...")
    z_year = ds_year.sel(pressure_level=500)["z"]
    if "valid_time" in z_year.dims:
        z_year = z_year.rename({"valid_time": "time"})

    month_idx = [int(m) - 1 for m in z_year.time.dt.month.values]
    clim_broadcast = clim_np[month_idx]           # time, latitude, longitude
    anom3d = z_year.values - clim_broadcast       

    anom3d_da = xr.DataArray(
        anom3d,
        dims=["time", "latitude", "longitude"],
        coords={
            "time":      z_year.time,
            "latitude":  z_year.latitude,
            "longitude": z_year.longitude,
        },
    )
    del z_year, anom3d, clim_broadcast

    # Calculate spatial means for each region
    result: Dict[str, Dict[str, np.ndarray]] = {}
    for name, area in AREAS.items():
        result[name] = {
            "time":       timestamps,
            "vort850":    crop_area_mean(vort850_3d, area).values,
            "vort500":    crop_area_mean(vort500_3d, area).values,
            "anom_gz500": crop_area_mean(anom3d_da,  area).values,
        }

    del vort850_3d, vort500_3d, anom3d_da, ds_year
    gc.collect()

    log.info(f"  [{year}] DONE ✓")
    return result


# Blocking criterion

def calculate_blockings(df: pd.DataFrame) -> pd.Series:
    """Apply the blocking criterion and return a daily boolean series."""
    all_positive = (df > 0).all(axis=1)
    indicator = pd.Series(False, index=df.index, name="blocking")
    in_blocking = False

    for i in range(len(all_positive)):
        positive_today = all_positive.iloc[i]

        if not in_blocking:
            window_start = max(0, i - PERSISTENCE_DAYS + 1)
            window = all_positive.iloc[window_start : i + 1]
            if len(window) == PERSISTENCE_DAYS and window.all():
                in_blocking = True
                indicator.iloc[window_start : i + 1] = True
        else:
            if positive_today:
                indicator.iloc[i] = True
            else:
                in_blocking = False

    return indicator


# Run processing

def main() -> None:

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: download ERA5 data
    log.info("=== Step 1: Download ERA5 ===")
    download_era5()

    # Step 2: open the ERA5 dataset
    log.info("=== Step 2: Opening ERA5 (lazy) ===")
    ds = xr.open_dataset(FILE_ERA5)
    log.info(f"  Dimensions : {dict(ds.dims)}")
    log.info(f"  Variables  : {list(ds.data_vars)}")

    # Step 3: load the monthly climatology
    log.info(f"=== Step 3: Climatology [{CLIM_PERIOD}] ===")
    if not FILE_CLIM_GZ.exists():
        raise FileNotFoundError(
            f"Climatology file not found: {FILE_CLIM_GZ}\n"
            "Generate it with generate_era5_climatology.py before continuing."
        )
    clim_gz = xr.open_dataset(FILE_CLIM_GZ)
    log.info(f"  Climatology dims : {dict(clim_gz.dims)}")
    log.info(f"  Variables        : {list(clim_gz.data_vars)}")

    # Align the climatology to the ERA5 grid and reuse it for all years.
    time_coord = "valid_time" if "valid_time" in ds.dims else "time"
    ds_ref = ds.isel({time_coord: 0})
    clim_aligned = clim_gz["z"].reindex_like(ds_ref.sel(pressure_level=500)["z"])
    clim_np = clim_aligned.values
    log.info(f"  Climatology loaded: shape {clim_np.shape}")

    # Step 4: process each year
    years = list(range(START_YEAR, END_YEAR + 1))
    total_years = len(years)
    log.info(f"=== Step 4: Processing {total_years} years ({START_YEAR}-{END_YEAR}) ===")

    accumulators: Dict[str, List[Dict[str, np.ndarray]]] = {name: [] for name in AREAS}

    for idx, year in enumerate(years, start=1):
        log.info(f"--- Year {year} ({idx}/{total_years}) ---")
        year_result = process_year(ds, year, clim_np)
        for name, data in year_result.items():
            accumulators[name].append(data)

    # Step 5: concatenate yearly series and apply the blocking criterion
    log.info("=== Step 5: Concatenating series and calculating blockings ===")
    consolidated_series: Dict[str, pd.Series] = {}

    for name in AREAS:
        slices = accumulators[name]
        if not slices:
            log.warning(f"  [{name}] no data, skipping.")
            continue

        times = np.concatenate([s["time"]        for s in slices])
        v850 = np.concatenate([s["vort850"]      for s in slices])
        v500 = np.concatenate([s["vort500"]      for s in slices])
        anom_gz = np.concatenate([s["anom_gz500"]   for s in slices])

        df = pd.DataFrame(
            {"vort850": v850, "vort500": v500, "anom_gz500": anom_gz},
            index=pd.DatetimeIndex(times, name="date"),
        ).dropna().sort_index()

        log.info(f"  [{name}] {df.index[0].date()} -> {df.index[-1].date()} "
                 f"({len(df)} days)")

        df.to_csv(OUTPUT_DIR / f"{name}_vars.csv", float_format="%.6e")

        blockings = calculate_blockings(df)
        n_days = int(blockings.sum())
        pct = 100.0 * n_days / len(blockings)
        log.info(f"    Blockings: {n_days} days ({pct:.1f}%)")

        consolidated_series[name] = blockings.astype(int)

    # Step 6: save the consolidated daily series
    log.info("=== Step 6: Generating consolidated series ===")
    df_consolidated = pd.DataFrame(consolidated_series)
    df_consolidated.index.name = "time"
    consolidated_path = OUTPUT_DIR / "daily_blocking_series.csv"
    df_consolidated.to_csv(consolidated_path)
    log.info(f"  Saved: {consolidated_path}")

    # Summary
    log.info("\n=== Final summary ===")
    log.info(f"  Climatology : {CLIM_PERIOD}")
    log.info(f"  Period      : {START_YEAR}-{END_YEAR}")
    log.info(f"  Output      : {OUTPUT_DIR}/")
    log.info(f"  {'Area':<12} {'Days':>6} {'%':>6}")
    log.info(f"  {'-'*26}")
    for name, series in consolidated_series.items():
        n = int(series.sum())
        pct = 100.0 * n / len(series)
        log.info(f"  {name:<12} {n:>6d} {pct:>5.1f}%")


if __name__ == "__main__":
    main()
