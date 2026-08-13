#!/usr/bin/env python3
"""
Preprocess CMIP6 atmospheric fields for the South Atlantic Convergence Zone (SACZ) index.

The script extracts the required pressure levels, computes divergence and
vorticity, calculates spatial means over the SACZ index regions, and saves
the resulting time series as CSV files.
"""

import sys
import warnings
from pathlib import Path
from typing import Any

import geopandas as gpd
import metpy.calc
import numpy as np
import xarray as xr

from riskclima_sacz.config import SACZSettings, parse_settings
from riskclima_sacz.libs import era5 as loaded_era5
from riskclima_sacz.libs import grid as loaded_grid

warnings.filterwarnings("ignore", category=FutureWarning)
xr.set_options(use_new_combine_kwarg_defaults=True)


# Configuration

SOURCE_ID = ""
EXPERIMENT_ID = ""
YEARS: list[int] = []
CMIP6_DATA_DIR = Path()
CMIP6_INPUT_DIR = Path()
YEAR = 0

# Project root
SACZ_BASE = Path()


SHAPE_PATH = Path()
areas: Any = None
grid_lib: Any = None
era5_lib: Any = None

# Map the ERA5-style variable names used by the index code to CMIP6 names.
ERA5_TO_CMIP6 = {
    "w": "wap",  # vertical pressure velocity (Pa/s)
    "z": "zg",  # geopotential height (m); CMIP6 zg is already expressed in metres
    "u": "ua",  # zonal wind (m/s)
    "v": "va",  # meridional wind (m/s)
}


def configure(settings: SACZSettings) -> None:
    """Apply shared settings to the existing preprocessing workflow."""
    global CMIP6_DATA_DIR, CMIP6_INPUT_DIR, EXPERIMENT_ID, SACZ_BASE
    global SHAPE_PATH, SOURCE_ID, YEARS, areas
    global era5_lib, grid_lib
    SACZ_BASE = settings.path(Path())
    CMIP6_DATA_DIR = settings.path(settings.cmip6_data_directory)
    CMIP6_INPUT_DIR = settings.path(settings.cmip6_input_directory)
    SOURCE_ID = settings.cmip6_source_id
    EXPERIMENT_ID = settings.cmip6_experiment_id
    YEARS = list(range(settings.cmip6_start_year, settings.cmip6_end_year + 1))
    SHAPE_PATH = settings.path(settings.areas_file)
    areas = gpd.read_file(SHAPE_PATH).set_index("area")
    era5_lib = loaded_era5
    grid_lib = loaded_grid


# Helper functions


def find_zarr(variable_id: str) -> Path | None:
    """Return the preferred available Zarr store for a variable."""
    base = CMIP6_DATA_DIR / SOURCE_ID / EXPERIMENT_ID / "day" / variable_id
    if not base.exists():
        return None
    for grid_dir in base.iterdir():
        if not grid_dir.is_dir():
            continue
        for candidate in ["ensemble_mean.zarr", "ensemble_all.zarr"]:
            p = grid_dir / candidate
            if p.exists():
                return p
        members = sorted(grid_dir.glob("member-*.zarr"))
        if members:
            return members[0]
    return None


def open_level(variable_id: str, level_hpa: int) -> xr.DataArray | None:
    """Open a CMIP6 field for the selected year and pressure level."""
    zarr_path = find_zarr(variable_id)
    if zarr_path is None:
        return None

    ds = xr.open_zarr(str(zarr_path))
    try:
        da = ds[variable_id].sel(time=str(YEAR))
    except KeyError:
        years = sorted(set(ds[variable_id]["time"].dt.year.values))
        raise ValueError(
            f"Year {YEAR} not found in '{variable_id}' "
            f"({EXPERIMENT_ID}). Available years: {years[0]}-{years[-1]}"
        ) from None

    plev_vals = da["plev"].values
    if plev_vals.size == 0:
        return None

    tol_pa = 100.0  # 1 hPa tolerance for floating-point comparisons
    plev_pa = float(level_hpa * 100)

    exact = np.where(np.abs(plev_vals - plev_pa) < tol_pa)[0]
    if exact.size > 0:
        selected_pa = plev_vals[exact[0]]
    elif level_hpa == 200:
        # Use 250 hPa as a proxy when 200 hPa is unavailable.
        alt_pa = 25000.0
        alt = np.where(np.abs(plev_vals - alt_pa) < tol_pa)[0]
        if alt.size == 0:
            available = sorted(plev_vals / 100)
            raise ValueError(
                f"200 hPa not found in '{variable_id}', and 250 hPa is also unavailable. "
                f"Available levels: {available} hPa"
            )
        selected_pa = plev_vals[alt[0]]
        print(f"  [proxy] {variable_id}: 200 hPa unavailable; using 250 hPa.")
    else:
        available = sorted(plev_vals / 100)
        raise ValueError(
            f"Level {level_hpa} hPa not found in '{variable_id}'. Available levels: {available} hPa"
        )

    da = da.sel(plev=selected_pa, drop=True)

    # MetPy requires wind units to compute divergence and vorticity.
    if variable_id in ("ua", "va") and "units" not in da.attrs:
        da = da.assign_attrs(units="m/s")

    # Match the 0-360 longitude convention used by the SACZ polygons.
    lon_name = next((c for c in da.coords if c in ("lon", "longitude")), "lon")
    new_lon = da[lon_name] % 360
    da = da.assign_coords({lon_name: new_lon}).sortby(lon_name)

    return da


# Divergence and vorticity

_div_vort_cache: dict[int, tuple[Any, Any]] = {}


def get_div_vort(level_hpa: int):
    """Compute and cache divergence and vorticity for one pressure level."""
    if level_hpa in _div_vort_cache:
        return _div_vort_cache[level_hpa]

    ua = open_level("ua", level_hpa)
    va = open_level("va", level_hpa)

    if ua is None or va is None:
        _div_vort_cache[level_hpa] = (None, None)
        return None, None

    ua_cf = ua.to_dataset(name="ua").metpy.parse_cf()["ua"]
    va_cf = va.to_dataset(name="va").metpy.parse_cf()["va"]

    div = (
        metpy.calc.divergence(ua_cf, va_cf)
        .metpy.dequantify()
        .drop_vars("metpy_crs", errors="ignore")
    )
    vort = (
        metpy.calc.vorticity(ua_cf, va_cf)
        .metpy.dequantify()
        .drop_vars("metpy_crs", errors="ignore")
    )

    _div_vort_cache[level_hpa] = (div, vort)
    return div, vort


# Process and save fields


def _spatial_dim_names(da: xr.DataArray):
    """Return (lon_name, lat_name) by inspecting coordinate names."""
    lon_name = next((c for c in da.coords if c in ("lon", "longitude")), "lon")
    lat_name = next((c for c in da.coords if c in ("lat", "latitude")), "lat")
    return lon_name, lat_name


def process_collection(collection: Any, output_dir: Path) -> None:
    """Average one variable/level collection over the SACZ index regions."""
    out_filename = f"{collection.out_name}{collection.level}.csv"
    out_path = output_dir / out_filename

    if out_path.exists():
        print(f"  {out_filename} already exists, skipping.")
        return

    print(f"  Processing {collection.variable} at {collection.level} hPa...")

    var = collection.variable

    if var == "div":
        da, _ = get_div_vort(collection.level)
    elif var == "vort":
        _, da = get_div_vort(collection.level)
    elif var == "z":
        # CMIP6 zg is already geopotential height in metres.
        da = open_level("zg", collection.level)
    else:
        da = open_level(ERA5_TO_CMIP6[var], collection.level)

    if da is None:
        raise ValueError(
            f"Variable '{collection.variable}' at {collection.level} hPa not available "
            f"for {SOURCE_ID}/{EXPERIMENT_ID}. Check that the zarr data was downloaded."
        )

    lon_name, lat_name = _spatial_dim_names(da)

    output_ds = []
    for feature in collection.features:
        if feature.shape not in areas.index:
            raise ValueError(
                f"Area '{feature.shape}' not found in shapefile "
                f"({SHAPE_PATH.name}). Check sams_index_calc_areas.shp."
            )

        print(f"    Clipping {feature.area} ({feature.shape})...")
        sliced = grid_lib.gridslice(
            da,
            areas.loc[feature.shape],
            xdim=lon_name,
            ydim=lat_name,
        )
        mean_ds = sliced.mean([lon_name, lat_name]).to_dataset(name=feature.area)
        output_ds.append(mean_ds)

    if not output_ds:
        raise ValueError(
            f"No spatial output produced for {out_filename}. "
            f"Check that the shapefile polygons overlap the model grid."
        )

    outdf = xr.merge(output_ds).to_dataframe().sort_index()
    outdf.drop(columns=["plev", "spatial_ref"], errors="ignore", inplace=True)
    outdf.index.name = "time"
    outdf.to_csv(out_path)
    print(f"    Saved → {out_path}")


# Run preprocessing


def process_year(year: int):
    global YEAR
    YEAR = year

    output_dir = CMIP6_INPUT_DIR / SOURCE_ID / EXPERIMENT_ID / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 60}")
    print(f"CMIP6 SACZ preprocessing: {SOURCE_ID} / {EXPERIMENT_ID} / {year}")
    print(f"Data dir : {CMIP6_DATA_DIR}")
    print(f"Output   : {output_dir}")
    print(f"{'=' * 60}\n")

    failed = []
    for collection in era5_lib.feature_collection:
        try:
            process_collection(collection, output_dir)
        except Exception as e:
            print(f"  ERROR [{collection.variable} {collection.level} hPa]: {e}")
            failed.append(f"{collection.out_name}{collection.level}.csv")

    if failed:
        print(f"\n[ERROR] Preprocessing incomplete — {len(failed)} file(s) not produced:")
        for f in failed:
            print(f"  - {f}")
        print("Fix the errors above before running main_sacz_index_cmip6.py.")
        sys.exit(1)

    print(f"\nCompleted: {SOURCE_ID} / {EXPERIMENT_ID} / {year}")

    _div_vort_cache.clear()


def main(arguments: list[str] | None = None) -> None:
    """Run CMIP6 preprocessing for every configured year."""
    settings = parse_settings(arguments)
    configure(settings)
    for year in YEARS:
        process_year(year)


if __name__ == "__main__":
    main()
