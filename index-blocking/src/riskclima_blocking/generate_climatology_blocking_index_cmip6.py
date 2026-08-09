#!/usr/bin/env python3
# coding: utf-8
"""
Generate a monthly 500 hPa geopotential height climatology from CMIP6 data.

The script reads historical CMIP6 geopotential height, selects the reference
period and pressure level, computes monthly climatological means, and saves
the resulting field as a NetCDF file.
"""

import logging
import warnings
from pathlib import Path

import numpy as np
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

# Reference climatology period
CLIM_START = 1980
CLIM_END = 2010
CLIM_LABEL = "80_10"

# Update this path to the directory containing the CMIP6 Zarr stores
CMIP6_DATA_DIR = Path("/path/to/CMIP6")


# Project root
BLOCKING_BASE = Path(__file__).resolve().parent
CLIM_OUTPUT_DIR = BLOCKING_BASE / "climatology_data"
CLIM_FILE = CLIM_OUTPUT_DIR / f"{SOURCE_ID}_clima_zg500_{CLIM_LABEL}.nc"
HIST_EXPERIMENT = "historical"


# Helper functions

def find_zarr(source_id: str, experiment_id: str, variable_id: str) -> Path | None:
    """Return the preferred available Zarr store for a CMIP6 variable."""
    base = CMIP6_DATA_DIR / source_id / experiment_id / "day" / variable_id
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
            f"All Zarr stores for '{variable_id}' ({source_id}/{experiment_id}) "
            f"have an empty plev dimension and cannot be used:\n"
            f"  {', '.join(skipped)}\n"
            f"The CMIP6 downloader may not have downloaded pressure-level data "
            f"for this variable/model combination."
        )
    return None


def select_level(da: xr.DataArray, level_hpa: int) -> xr.DataArray:
    """Select the pressure level closest to level_hpa."""
    plev_vals = da["plev"].values
    plev_pa = float(level_hpa * 100)
    closest = plev_vals[np.argmin(np.abs(plev_vals - plev_pa))]
    log.info(
        f"  Level {level_hpa} hPa → closest plev = {closest/100:.0f} hPa ({closest:.0f} Pa)"
    )
    return da.sel(plev=closest, drop=True)


# Generate climatology

def main() -> None:
    if CLIM_FILE.exists():
        log.info(f"Climatology already exists, skipping: {CLIM_FILE}")
        return

    CLIM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 65)
    log.info(f"CMIP6 zg500 climatology — {SOURCE_ID}  ({CLIM_START}–{CLIM_END})")
    log.info("=" * 65)

    zarr_path = find_zarr(SOURCE_ID, HIST_EXPERIMENT, "zg")
    if zarr_path is None:
        raise FileNotFoundError(
            f"Variable 'zg' not downloaded for {SOURCE_ID}/{HIST_EXPERIMENT}.\n"
            f"Expected directory: {CMIP6_DATA_DIR}/{SOURCE_ID}/{HIST_EXPERIMENT}/day/zg/"
        )
    log.info(f"Zarr path : {zarr_path}")

    ds = xr.open_zarr(str(zarr_path))
    da_zg = ds["zg"]
    log.info(f"Full dims : {dict(ds.sizes)}")
    log.info(f"Time range: {da_zg.time.values[0]} → {da_zg.time.values[-1]}")

    # Select the reference period
    da_clim = da_zg.sel(time=slice(str(CLIM_START), str(CLIM_END)))
    n_days = da_clim.sizes["time"]
    log.info(f"Climatology period: {n_days} days ({CLIM_START}–{CLIM_END})")

    if n_days == 0:
        raise ValueError(
            f"No data found for {CLIM_START}–{CLIM_END} in {zarr_path}.\n"
            f"Check that the historical Zarr covers this period."
        )

    # Select 500 hPa
    da_500 = select_level(da_clim, 500)

    # Compute one climatological mean for each calendar month
    log.info("Computing monthly climatology (groupby time.month)...")
    clim = da_500.groupby("time.month").mean("time")
    clim.name = "zg"
    clim.attrs["long_name"] = "Geopotential height monthly climatology 500 hPa"
    clim.attrs["units"] = da_zg.attrs.get("units", "m")
    clim.attrs["source_id"] = SOURCE_ID
    clim.attrs["clim_period"] = f"{CLIM_START}-{CLIM_END}"

    clim_ds = clim.to_dataset()
    clim_ds.to_netcdf(CLIM_FILE)

    log.info(f"Saved : {CLIM_FILE}")
    log.info(f"Dims  : {dict(clim_ds.dims)}")
    log.info(f"zg range : {float(clim.min()):.1f} – {float(clim.max()):.1f} m")
    log.info("Done.")


if __name__ == "__main__":
    main()
