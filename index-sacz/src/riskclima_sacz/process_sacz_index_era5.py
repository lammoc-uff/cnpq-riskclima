"""
Download and preprocess ERA5 atmospheric fields for the South Atlantic
Convergence Zone (SACZ) index.

The script downloads the required pressure-level fields, computes daily means,
derives wind divergence and vorticity, aggregates the data into yearly files,
and saves spatially averaged time series for the SACZ index regions.
"""

import calendar
import os
import shutil
import subprocess
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any

import geopandas as gpd
import metpy.calc
import numpy as np
import xarray as xr

from riskclima_sacz.config import SACZSettings, parse_settings

# Project root
SACZ_BASE = Path()
areas: Any = None
grid: Any = None
era5: Any = None
ERA5_YEAR = None
ERA5_INPUT_DIR = Path()
ERA5_RAW_DIR = Path()

# ERA5 configuration
bucket = "gs://gcp-public-data-arco-era5"
variables_config = [
    ("vertical_velocity", "w", 500),
    ("geopotential", "z", 500),
    ("u_component_of_wind", "u", 200),
    ("v_component_of_wind", "v", 200),
    ("u_component_of_wind", "u", 850),
    ("v_component_of_wind", "v", 850),
]
gsutil_command = "gsutil -q cp"


def configure(settings: SACZSettings) -> None:
    """Apply shared settings to the ERA5 preprocessing workflow."""
    global ERA5_INPUT_DIR, ERA5_RAW_DIR, ERA5_YEAR, SACZ_BASE, areas
    global era5, grid
    if settings.era5_year is None:
        raise ValueError("ERA5_YEAR must be set in .env or supplied with --era5-year")
    SACZ_BASE = settings.path(Path())
    ERA5_RAW_DIR = settings.path(settings.era5_raw_directory)
    ERA5_INPUT_DIR = settings.path(settings.era5_input_directory)
    ERA5_YEAR = settings.era5_year
    areas = gpd.read_file(settings.path(settings.areas_file)).set_index("area")
    from libs import era5 as loaded_era5
    from libs import grid as loaded_grid

    era5 = loaded_era5
    grid = loaded_grid


def create_directories(raw_directory: Path, input_directory: Path, year: int) -> tuple[Path, Path]:
    year_path_nc = raw_directory / str(year) / "nc"
    postproc_data_path = input_directory / str(year)
    year_path_nc.mkdir(parents=True, exist_ok=True)
    postproc_data_path.mkdir(parents=True, exist_ok=True)
    return year_path_nc, postproc_data_path


def download_file_era5(
    year: int, month: int, day: int, var: str, shortname: str, level: int, outdir: Path
) -> tuple[bool, Path]:
    date_path = f"{year:04d}/{month:02d}/{day:02d}"
    remote_path = f"{bucket}/raw/date-variable-pressure_level/{date_path}/{var}/{level}.nc"
    local_filename = f"{shortname}_{level}_{year}{month:02d}{day:02d}.nc"
    local_path = outdir / local_filename
    if local_path.exists():
        print(f"File {local_path} already exists, skipping download.")
        return True, local_path
    command = f"{gsutil_command} {remote_path} {local_path}"
    result = os.system(command)
    return (result == 0, local_path)


def calculate_daily_mean(input_file: Path, output_file: Path) -> None:
    if output_file.exists():
        return

    # Calculate the daily mean.
    subprocess.run(["cdo", "-b", "F32", "daymean", str(input_file), str(output_file)], check=True)
    input_file.unlink()


def fix_level_metadata(filepath: Path, level: int) -> None:
    """Set pressure-level metadata required by CDO."""
    temp_file = str(filepath) + ".levfix.nc"
    try:
        with xr.open_dataset(filepath) as ds:
            ds = ds.assign_coords(level=np.float64(level))
            ds["level"].attrs = {
                "units": "hPa",
                "long_name": "pressure_level",
                "standard_name": "air_pressure",
                "axis": "Z",
                "positive": "down",
            }
            ds.to_netcdf(temp_file)
        Path(temp_file).replace(filepath)
    finally:
        temporary_path = Path(temp_file)
        if temporary_path.exists():
            temporary_path.unlink()


def convert_geopotential_to_height(z_nc_path: Path) -> None:
    """Convert geopotential (m²/s²) to geopotential height (m)."""
    temp_file = str(z_nc_path) + ".temp"
    try:
        with xr.open_dataset(z_nc_path) as ds:
            if "z" in ds:
                new_ds = ds.copy()
                new_ds["hgt"] = ds["z"] / 9.80665
                new_ds["hgt"].attrs.update(
                    {
                        "units": "m",
                        "long_name": "Geopotential Height",
                        "standard_name": "geopotential_height",
                    }
                )
                new_ds = new_ds.drop_vars("z")
                new_ds.to_netcdf(temp_file)
                ds.close()
                Path(temp_file).replace(z_nc_path)
            else:
                print(f"Warning: Variable 'z' not found in {z_nc_path}")
    except Exception as e:
        print(f"Error converting geopotential to height: {e!s}")
        temporary_path = Path(temp_file)
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    finally:
        temporary_path = Path(temp_file)
        if temporary_path.exists():
            temporary_path.unlink()


def calculate_wind_vorticity_divergence(
    uwnd_file: Path, vwnd_file: Path, vorpath: Path, divpath: Path, level: int
) -> None:
    if vorpath.exists() and divpath.exists():
        return
    uds = xr.open_dataset(uwnd_file).metpy.parse_cf().u
    vds = xr.open_dataset(vwnd_file).metpy.parse_cf().v

    if not divpath.exists():
        div = metpy.calc.divergence(uds, vds).metpy.dequantify().drop_vars("metpy_crs")
        div = div.assign_coords(level=level)
        div = div.expand_dims("level")
        div = div.transpose("time", "level", "latitude", "longitude")  # time first
        div = div.to_dataset(name="div")
        div.to_netcdf(divpath)

    if not vorpath.exists():
        vor = metpy.calc.vorticity(uds, vds).metpy.dequantify().drop_vars("metpy_crs")
        vor = vor.assign_coords(level=level)
        vor = vor.expand_dims("level")
        vor = vor.transpose("time", "level", "latitude", "longitude")  # time first
        vor = vor.to_dataset(name="vort")
        vor.to_netcdf(vorpath)


def process_day(args: tuple[int, int, int, Path]) -> None:
    year, month, day, year_path_nc = args
    date_str = f"{year}{month:02d}{day:02d}"
    print(f"\nProcessing {year}-{month:02d}-{day:02d}")
    downloaded_files = {}
    for var_full, shortname, level in variables_config:
        success, filepath = download_file_era5(
            year, month, day, var_full, shortname, level, year_path_nc
        )
        if success:
            daily_file = year_path_nc / f"{shortname}_{level}_{date_str}_daily.nc"
            calculate_daily_mean(filepath, daily_file)
            fix_level_metadata(daily_file, level)
            if shortname == "z":
                convert_geopotential_to_height(daily_file)
            downloaded_files[f"{shortname}_{level}"] = daily_file
    for level in [200, 850]:
        u_key = f"u_{level}"
        v_key = f"v_{level}"
        if u_key in downloaded_files and v_key in downloaded_files:
            uwnd_file = downloaded_files[u_key]
            vwnd_file = downloaded_files[v_key]
            vorpath = year_path_nc / f"vort_{level}_{date_str}.nc"
            divpath = year_path_nc / f"div_{level}_{date_str}.nc"
            calculate_wind_vorticity_divergence(uwnd_file, vwnd_file, vorpath, divpath, level)


def merge_daily_files(year_path_nc: Path, year: int) -> None:
    """Merge daily files into yearly files for each variable and pressure level."""
    output_names = {
        "u": f"uwnd{year}.nc",
        "v": f"vwnd{year}.nc",
        "w": f"w{year}.nc",
        "hgt": f"hgt{year}.nc",
        "div": f"div{year}.nc",
        "vort": f"vort{year}.nc",
    }

    # Daily-file patterns generated by process_day.

    daily_patterns = {
        "w": ([("w_500_*_daily.nc", [500])], False),
        "hgt": (
            [("z_500_*_daily.nc", [500])],
            False,
        ),  # converted to geopotential height in process_day
        "u": ([("u_200_*_daily.nc", [200]), ("u_850_*_daily.nc", [850])], True),
        "v": ([("v_200_*_daily.nc", [200]), ("v_850_*_daily.nc", [850])], True),
        "div": (
            [
                ("div_200_*_daily.nc", [200]),  # level-specific pattern
                ("div_850_*_daily.nc", [850]),
            ],
            True,
        ),
        "vort": (
            [
                ("vort_200_*_daily.nc", [200]),  # level-specific pattern
                ("vort_850_*_daily.nc", [850]),
            ],
            True,
        ),
    }

    # Divergence and vorticity files are written without the _daily suffix.
    daily_patterns["div"] = ([("div_200_*.nc", [200]), ("div_850_*.nc", [850])], True)
    daily_patterns["vort"] = ([("vort_200_*.nc", [200]), ("vort_850_*.nc", [850])], True)

    temp_dir = year_path_nc / "temp_merge"
    temp_dir.mkdir(exist_ok=True)

    try:
        for var_name, (level_patterns, has_multiple_levels) in daily_patterns.items():
            output_file = year_path_nc / output_names[var_name]
            if output_file.exists():
                print(f"  {output_file.name} already exists, skipping merge.")
                continue

            print(f"\nMerging {var_name} files...")
            level_files = []

            for pattern, levels in level_patterns:
                daily_files = sorted(year_path_nc.glob(pattern))

                # Exclude files already created in the temporary merge directory.
                daily_files = [f for f in daily_files if f.parent != temp_dir]

                if not daily_files:
                    print(f"  No files found for pattern '{pattern}', skipping.")
                    continue

                level_label = levels[0]
                level_file = temp_dir / f"{var_name}_{level_label}_merged.nc"
                print(f"  Merging {len(daily_files)} files for level {level_label} hPa...")
                subprocess.run(
                    ["cdo", "-b", "F32", "mergetime", *map(str, daily_files), str(level_file)],
                    check=True,
                )
                # Geopotential height was already converted in process_day.
                level_files.append((level_file, daily_files, pattern))

            if not level_files:
                print(f"  No level files produced for {var_name}, skipping.")
                continue

            if has_multiple_levels and len(level_files) > 1:
                print("  Merging levels into single file...")
                subprocess.run(
                    [
                        "cdo",
                        "-b",
                        "F32",
                        "merge",
                        *[str(lf) for lf, _, _ in level_files],
                        str(output_file),
                    ],
                    check=True,
                )
            else:
                # Move single-level output directly.
                shutil.move(str(level_files[0][0]), str(output_file))

            # Remove only the daily files included in the successful merge.
            if output_file.exists():
                for _, daily_files_used, _ in level_files:
                    for f in daily_files_used:
                        if f.exists():
                            f.unlink()

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def process_collection(
    collection: Any, postproc_data_path: Path, year_path_nc: Path, area_index: Any, year: int
) -> None:
    ds_filename = f"{collection.out_name}{collection.level}.csv"
    ds_output_path = postproc_data_path / ds_filename
    if ds_output_path.exists():
        print(f"{ds_output_path.stem} already exists locally.")
        return

    if collection.variable == "z":
        yearly_file = year_path_nc / f"hgt{year}.nc"
        var_name = "hgt"
    elif collection.variable in ["u", "v"]:
        yearly_file = year_path_nc / f"{collection.out_name}{year}.nc"
        var_name = collection.variable
    else:
        yearly_file = year_path_nc / f"{collection.variable}{year}.nc"
        var_name = collection.variable

    if not yearly_file.exists():
        print(f"Warning: {yearly_file} not found!")
        return

    print(f"Processing {collection.variable} at {collection.level}hPa...")
    output_ds = []

    with xr.open_dataset(yearly_file) as ds:
        level_data = (
            ds.sel(level=collection.level)[var_name] if "level" in ds.dims else ds[var_name]
        )
        for feature in collection.features:
            print(f"Calculating {collection.variable} {collection.level}hPa for {feature.shape}...")
            if feature.shape not in area_index.index:
                print(f"Warning: Area '{feature.shape}' not found in shapefile!")
                continue
            data = (
                grid.gridslice(
                    level_data,
                    area_index.loc[feature.shape],
                    xdim="longitude",
                    ydim="latitude",
                )
                .mean(["longitude", "latitude"])
                .to_dataset(name=feature.area)
            )
            output_ds.append(data)

    if output_ds:
        outdf = xr.merge(output_ds).to_dataframe().sort_index()
        outdf.drop(columns=["level", "spatial_ref"], errors="ignore", inplace=True)
        outdf.index.name = "time"
        outdf.to_csv(ds_output_path)
        print(f"Saved to {ds_output_path}")


def main(arguments: list[str] | None = None) -> None:
    """Download and preprocess ERA5 data for the configured year."""
    settings = parse_settings(arguments)
    configure(settings)
    if ERA5_YEAR is None or areas is None or era5 is None:
        raise RuntimeError("ERA5 preprocessing was not configured")
    year = ERA5_YEAR
    print(f"\nProcessing year {year}")
    year_path_nc, postproc_data_path = create_directories(ERA5_RAW_DIR, ERA5_INPUT_DIR, year)

    process_args = [
        (year, month, day, year_path_nc)
        for month in range(1, 13)
        for day in range(1, calendar.monthrange(year, month)[1] + 1)
    ]

    num_processes = max(1, cpu_count() // 2)
    print(f"Using {num_processes} processes for parallel processing")

    with Pool(num_processes) as pool:
        pool.map(process_day, process_args)

    merge_daily_files(year_path_nc, year)

    for collection in era5.feature_collection:
        process_collection(collection, postproc_data_path, year_path_nc, areas, year)


if __name__ == "__main__":
    main()
