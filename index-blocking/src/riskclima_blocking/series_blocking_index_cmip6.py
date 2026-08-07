#!/usr/bin/env python3
# coding: utf-8
"""
Calculate daily atmospheric blocking series from CMIP6 data.

The script computes relative vorticity at 850 and 500 hPa, calculates 500 hPa
geopotential height anomalies relative to a monthly climatology, applies the
blocking persistence criterion, and saves daily blocking indicators by region.
"""

import gc
import logging
import warnings
from pathlib import Path
from typing import Dict, List, Tuple

import metpy.calc as mpcalc
import numpy as np
import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore", category=FutureWarning)
xr.set_options(use_new_combine_kwarg_defaults=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Configuration
SOURCE_ID = "BCC-CSM2-MR"
EXPERIMENT_ID = "ssp585"

YEARS = list(range(2015, 2051))

PERSISTENCE_DAYS = 3
CLIM_LABEL = "80_10"

# Update this path to the directory containing the CMIP6 Zarr stores.
CMIP6_DATA_DIR = Path("/path/to/CMIP6")


# Project root
BLOCKING_BASE = Path(__file__).resolve().parent
OUTPUT_DIR = BLOCKING_BASE / "output" / SOURCE_ID / EXPERIMENT_ID
CLIM_FILE = (
    BLOCKING_BASE / "climatology_data"
    / f"{SOURCE_ID}_clima_zg500_{CLIM_LABEL}.nc"
)


# Geographic areas
AREAS: Dict[str, Dict[str, float]] = {
    "total":    {"lat_min": -25.0, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -40.0},
    "north":    {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -40.0},
    "north_h1": {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -60.0, "lon_max": -50.0},
    "north_h2": {"lat_min": -17.5, "lat_max": -10.0, "lon_min": -50.0, "lon_max": -40.0},
    "south":      {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -60.0, "lon_max": -40.0},
    "south_h1":   {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -60.0, "lon_max": -50.0},
    "sout_h2":   {"lat_min": -25.0, "lat_max": -17.5, "lon_min": -50.0, "lon_max": -40.0},
}


# Helper functions

def find_zarr(variable_id: str) -> Path | None:
    """Return the preferred available Zarr store for a CMIP6 variable."""
    base = CMIP6_DATA_DIR / SOURCE_ID / EXPERIMENT_ID / "day" / variable_id
    if not base.exists():
        return None
    skipped = []
    for grid_dir in base.iterdir():
        if not grid_dir.is_dir():
            continue
        candidates = []
        for name in ["ensemble_mean.zarr", "ensemble_all.zarr"]:
            p = grid_dir / name
            if p.exists():
                candidates.append(p)
        candidates += sorted(grid_dir.glob("member-*.zarr"))
        for p in candidates:
            try:
                ds = xr.open_zarr(str(p))
                if ds[variable_id].sizes.get("plev", 1) > 0:
                    return p
                skipped.append(p.name)
                log.warning(f"  Skipping {p.name}: plev dimension is empty.")
            except Exception as e:
                skipped.append(p.name)
                log.warning(f"  Skipping {p.name}: {e}")
    if skipped:
        raise ValueError(
            f"All Zarr stores for '{variable_id}' ({SOURCE_ID}/{EXPERIMENT_ID}) "
            f"have an empty plev dimension and cannot be used:\n"
            f"  {', '.join(skipped)}\n"
            f"The CMIP6 downloader may not have downloaded pressure-level data "
            f"for this variable/model combination."
        )
    return None


def open_zarr_var(variable_id: str) -> xr.DataArray | None:
    """Open a CMIP6 Zarr store and return the requested DataArray lazily."""
    zarr_path = find_zarr(variable_id)
    if zarr_path is None:
        return None
    ds = xr.open_zarr(str(zarr_path))
    return ds[variable_id]


def _spatial_coords(da: xr.DataArray) -> Tuple[str, str]:
    """Return the latitude and longitude coordinate names."""
    lat_name = next((c for c in da.coords if c in ("lat", "latitude")), "lat")
    lon_name = next((c for c in da.coords if c in ("lon", "longitude")), "lon")
    return lat_name, lon_name


def select_level(da: xr.DataArray, level_hpa: int) -> xr.DataArray:
    """Select the pressure level closest to level_hpa."""
    plev_pa = float(level_hpa * 100)
    closest = da["plev"].values[np.argmin(np.abs(da["plev"].values - plev_pa))]
    return da.sel(plev=closest, drop=True)


def crop_area_mean(da: xr.DataArray, area: Dict[str, float]) -> xr.DataArray:
    """Crop to geographic bounds and return the spatial-mean time series."""
    lat_name, lon_name = _spatial_coords(da)
    lat_ok = (da[lat_name] >= area["lat_min"]) & (da[lat_name] <= area["lat_max"])
    lon_ok = (da[lon_name] >= area["lon_min"]) & (da[lon_name] <= area["lon_max"])
    return da.where(lat_ok & lon_ok, drop=True).mean(dim=[lat_name, lon_name])


# Vorticity

def compute_vorticity(
    da_ua: xr.DataArray, da_va: xr.DataArray
) -> xr.DataArray:
    """Compute relative vorticity from zonal and meridional wind."""
    if "units" not in da_ua.attrs:
        da_ua = da_ua.assign_attrs(units="m/s")
    if "units" not in da_va.attrs:
        da_va = da_va.assign_attrs(units="m/s")

    ua_cf = da_ua.to_dataset(name="ua").metpy.parse_cf()["ua"]
    va_cf = da_va.to_dataset(name="va").metpy.parse_cf()["va"]

    vort = (
        mpcalc.vorticity(ua_cf, va_cf)
        .metpy.dequantify()
        .drop_vars("metpy_crs", errors="ignore")
    )
    return vort


# Process one year

def process_year(
    da_ua: xr.DataArray,
    da_va: xr.DataArray,
    da_zg: xr.DataArray,
    year: int,
    clim_np: np.ndarray,    # shape (12, lat, lon) — monthly climatology
    clim_lat: np.ndarray,
    clim_lon: np.ndarray,
) -> Dict[str, Dict]:
    """Compute yearly blocking predictors and spatial means for each region."""
    year_str = str(year)

    # Select the target year
    ua850_yr = select_level(da_ua, 850).sel(time=year_str).load()
    ua500_yr = select_level(da_ua, 500).sel(time=year_str).load()
    va850_yr = select_level(da_va, 850).sel(time=year_str).load()
    va500_yr = select_level(da_va, 500).sel(time=year_str).load()
    zg500_yr = select_level(da_zg, 500).sel(time=year_str).load()

    n_days = ua850_yr.sizes["time"]
    if n_days == 0:
        log.warning(f"  [{year}] no timesteps found, skipping.")
        return {}

    timestamps = pd.to_datetime(ua850_yr.time.values).normalize()
    log.info(f"  [{year}] {n_days} days  ({timestamps[0].date()} → {timestamps[-1].date()})")

    # Compute 850 hPa vorticity
    log.info(f"  [{year}] vorticity 850 hPa...")
    vort850_3d = compute_vorticity(ua850_yr, va850_yr)
    del ua850_yr, va850_yr

    # Compute 500 hPa vorticity
    log.info(f"  [{year}] vorticity 500 hPa...")
    vort500_3d = compute_vorticity(ua500_yr, va500_yr)
    del ua500_yr, va500_yr

    # Compute the 500 hPa geopotential height anomaly
    log.info(f"  [{year}] zg500 anomaly...")
    lat_name, lon_name = _spatial_coords(zg500_yr)

    clim_da = xr.DataArray(
        clim_np,
        dims=["month", lat_name, lon_name],
        coords={
            "month":   np.arange(1, 13),
            lat_name:  clim_lat,
            lon_name:  clim_lon,
        },
    )
    # Match each day to its monthly climatology
    clim_broadcast = clim_da.sel(month=zg500_yr.time.dt.month)
    # Align climatology coordinates to the model grid.
    clim_broadcast = clim_broadcast.reindex(
        {lat_name: zg500_yr[lat_name], lon_name: zg500_yr[lon_name]},
        method="nearest",
    )
    anom3d = zg500_yr - clim_broadcast.values
    del zg500_yr, clim_broadcast

    # Calculate spatial means for each region
    result: Dict[str, Dict] = {}
    for name, area in AREAS.items():
        result[name] = {
            "time":       timestamps,
            "vort850":    crop_area_mean(vort850_3d, area).values,
            "vort500":    crop_area_mean(vort500_3d, area).values,
            "anom_zg500": crop_area_mean(anom3d,    area).values,
        }

    del vort850_3d, vort500_3d, anom3d
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

    log.info("=" * 65)
    log.info(f"CMIP6 blocking series — {SOURCE_ID} / {EXPERIMENT_ID}")
    log.info(f"Years: {YEARS[0]}–{YEARS[-1]}  |  Climatology: {CLIM_LABEL}")
    log.info("=" * 65)

    # Step 1: Open Zarr stores
    log.info("=== Step 1: Opening Zarr stores ===")
    da_ua = open_zarr_var("ua")
    da_va = open_zarr_var("va")
    da_zg = open_zarr_var("zg")

    missing = [v for v, d in [("ua", da_ua), ("va", da_va), ("zg", da_zg)] if d is None]
    if missing:
        raise FileNotFoundError(
            f"Variables not found for {SOURCE_ID}/{EXPERIMENT_ID}: {missing}\n"
            f"Check {CMIP6_DATA_DIR}/{SOURCE_ID}/{EXPERIMENT_ID}/day/"
        )

    log.info(f"  ua: {dict(da_ua.sizes)}")
    log.info(f"  va: {dict(da_va.sizes)}")
    log.info(f"  zg: {dict(da_zg.sizes)}")

    # Step 2: load climatology
    log.info(f"=== Step 2: Loading climatology [{CLIM_LABEL}] ===")
    if not CLIM_FILE.exists():
        raise FileNotFoundError(
            f"Climatology file not found: {CLIM_FILE}\n"
            "Generate it with cmip6_generate_climatology.py first."
        )

    clim_ds = xr.open_dataset(CLIM_FILE)
    clim_da = clim_ds["zg"]   # dims: month (1–12), lat, lon

    lat_name_c = next((c for c in clim_da.coords if c in ("lat", "latitude")), "lat")
    lon_name_c = next((c for c in clim_da.coords if c in ("lon", "longitude")), "lon")

    clim_np = clim_da.values
    clim_lat = clim_da[lat_name_c].values
    clim_lon = clim_da[lon_name_c].values
    log.info(f"  Shape: {clim_np.shape}  (month, {lat_name_c}, {lon_name_c})")

    # Step 3: process each year
    log.info(f"=== Step 3: Processing {len(YEARS)} years ===")
    accumulators: Dict[str, List[Dict]] = {name: [] for name in AREAS}

    for idx, year in enumerate(YEARS, start=1):
        log.info(f"--- Year {year}  ({idx}/{len(YEARS)}) ---")
        year_result = process_year(da_ua, da_va, da_zg, year, clim_np, clim_lat, clim_lon)
        for name, data in year_result.items():
            accumulators[name].append(data)

    # Step 4: concatenate the predictors and apply the blocking criterion
    log.info("=== Step 4: Concatenating series and detecting blockings ===")
    consolidated_series: Dict[str, pd.Series] = {}

    for name in AREAS:
        slices = accumulators[name]
        if not slices:
            log.warning(f"  [{name}] no data, skipping.")
            continue

        times = np.concatenate([s["time"]       for s in slices])
        v850 = np.concatenate([s["vort850"]     for s in slices])
        v500 = np.concatenate([s["vort500"]     for s in slices])
        anom_zg = np.concatenate([s["anom_zg500"]  for s in slices])

        df = pd.DataFrame(
            {"vort850": v850, "vort500": v500, "anom_zg500": anom_zg},
            index=pd.DatetimeIndex(times, name="date"),
        ).dropna().sort_index()

        log.info(
            f"  [{name}] {df.index[0].date()} → {df.index[-1].date()} "
            f"({len(df)} days)"
        )

        df.to_csv(OUTPUT_DIR / f"{name}_vars.csv", float_format="%.6e")

        blockings = calculate_blockings(df)
        n_days = int(blockings.sum())
        pct = 100.0 * n_days / len(blockings)
        log.info(f"    Blockings: {n_days} days ({pct:.1f}%)")

        consolidated_series[name] = blockings.astype(int)

    # Step 5: save the consolidated daily series
    log.info("=== Step 5: Saving daily_blocking_series.csv ===")
    df_final = pd.DataFrame(consolidated_series)
    df_final.index.name = "time"
    out_path = OUTPUT_DIR / "daily_blocking_series.csv"
    df_final.to_csv(out_path)
    log.info(f"  Saved: {out_path}")

    # Summary
    log.info("\n=== Final summary ===")
    log.info(f"  Model       : {SOURCE_ID}")
    log.info(f"  Experiment  : {EXPERIMENT_ID}")
    log.info(f"  Climatology : {CLIM_LABEL}")
    log.info(f"  Period      : {YEARS[0]}–{YEARS[-1]}")
    log.info(f"  Output      : {OUTPUT_DIR}")
    log.info(f"  {'Area':<12} {'Days':>6} {'%':>6}")
    log.info(f"  {'-'*26}")
    for name, series in consolidated_series.items():
        n = int(series.sum())
        pct = 100.0 * n / len(series)
        log.info(f"  {name:<12} {n:>6d} {pct:>5.1f}%")


if __name__ == "__main__":
    main()
