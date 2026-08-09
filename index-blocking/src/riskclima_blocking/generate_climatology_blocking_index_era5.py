"""
Generate monthly ERA5 climatologies for the atmospheric blocking workflow.

The script downloads ERA5 monthly means, extracts 500 hPa geopotential,
computes relative vorticity at 500 and 850 hPa, and saves monthly
climatological fields for the selected reference period.
"""

import logging
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path
from typing import List, Tuple

import cdsapi
import metpy.calc as mpcalc
import xarray as xr

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuration
CLIM_PERIOD = "60_90"
START_YEAR = 1960
END_YEAR = 1990

DATA_DIR = Path("climatology_data")

# Geographic domain: [north, west, south, east]
AREA = [10, -70, -35, -30]

# Pressure levels downloaded and processed
LEVELS = ["500", "850"]

# Limit parallel downloads to five workers
N_WORKERS = min(5, max(1, cpu_count() // 2))

# Paths
FILE_CONCAT_MONTHLY = DATA_DIR / f"era5_monthly_{CLIM_PERIOD}.nc"
FILE_GZ_500 = DATA_DIR / f"gz500_monthly_{CLIM_PERIOD}.nc"
FILE_VORT_RAW = DATA_DIR / f"vort_monthly_raw_{CLIM_PERIOD}.nc"

FILE_CLIM_GZ = DATA_DIR / f"clima_gz_{CLIM_PERIOD}.nc"
FILE_CLIM_VORT = DATA_DIR / f"clima_vort_{CLIM_PERIOD}.nc"


# Download ERA5 monthly means

def _worker_download(args: Tuple[int, Path]) -> Tuple[int, Path]:
    """Download ERA5 monthly means for one year."""
    year, data_dir = args
    file = data_dir / f"_monthly_{CLIM_PERIOD}_{year}.nc"
    if file.exists():
        return year, file

    c = cdsapi.Client(quiet=True)
    c.retrieve(
        "reanalysis-era5-pressure-levels-monthly-means",
        {
            "product_type": ["monthly_averaged_reanalysis"],
            "variable": [
                "geopotential",
                "u_component_of_wind",
                "v_component_of_wind",
            ],
            "pressure_level": LEVELS,
            "year":  [str(year)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "time":  ["00:00"],
            "area":  AREA,
            "data_format": "netcdf",
            "download_format": "unarchived",
        },
        str(file),
    )
    return year, file


def download_monthly_means() -> List[Path]:
    """Download yearly ERA5 monthly-mean files in parallel."""
    if FILE_CONCAT_MONTHLY.exists():
        log.info(f"Concatenated file already exists: {FILE_CONCAT_MONTHLY}. Skipping download.")
        return []

    years = list(range(START_YEAR, END_YEAR + 1))
    already_cached = sum(
        1 for y in years if (DATA_DIR / f"_monthly_{CLIM_PERIOD}_{y}.nc").exists()
    )
    log.info(
        f"Download: {len(years)} years ({START_YEAR}–{END_YEAR}), "
        f"{already_cached} cached, {len(years) - already_cached} to download "
        f"with {N_WORKERS} workers."
    )

    results = {}
    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {
            executor.submit(_worker_download, (year, DATA_DIR)): year
            for year in years
        }
        for future in as_completed(futures):
            year = futures[future]
            try:
                year_ret, path = future.result()
                results[year_ret] = path
                log.info(f"  {year_ret} completed.")
            except Exception as exc:
                log.error(f"  Error for year {year}: {exc}")
                raise

    return [results[y] for y in years]


# Concatenate annual files

def concatenate_files(files: List[Path]) -> xr.Dataset:
    """Concatenate annual ERA5 files into one monthly dataset."""
    if FILE_CONCAT_MONTHLY.exists():
        log.info(f"Opening concatenated file: {FILE_CONCAT_MONTHLY}")
        return xr.open_dataset(FILE_CONCAT_MONTHLY)

    log.info(f"Concatenating {len(files)} annual files...")
    ds = xr.open_mfdataset(files, combine="by_coords")
    ds.to_netcdf(FILE_CONCAT_MONTHLY)
    log.info(f"Concatenation saved: {FILE_CONCAT_MONTHLY}")
    return xr.open_dataset(FILE_CONCAT_MONTHLY)


# Extract 500 hPa geopotential

def extract_gz500(ds: xr.Dataset) -> xr.Dataset:
    """Extract 500 hPa geopotential and save it as an intermediate file."""
    if FILE_GZ_500.exists():
        log.info(f"gz500 already exists: {FILE_GZ_500}. Skipping.")
        return xr.open_dataset(FILE_GZ_500)

    log.info("Extracting geopotential (z) at 500 hPa...")

    dim_level = next(
        d for d in ["pressure_level", "level", "plev"] if d in ds.dims
    )
    dim_time = next(
        d for d in ["valid_time", "time"] if d in ds.dims or d in ds.coords
    )

    gz500 = ds["z"].sel({dim_level: 500})
    gz500.attrs["long_name"] = "Geopotential"
    gz500.attrs["units"] = "m2 s-2"

    ds_gz = gz500.to_dataset()

    if dim_time != "time":
        ds_gz = ds_gz.rename({dim_time: "time"})

    ds_gz.to_netcdf(FILE_GZ_500)
    log.info(f"gz500 saved: {FILE_GZ_500}")
    return ds_gz


# Relative vorticity

def calculate_vorticity(ds: xr.Dataset) -> xr.Dataset:
    """Calculate relative vorticity at 500 and 850 hPa with MetPy."""
    if FILE_VORT_RAW.exists():
        log.info(f"Raw vorticity already exists: {FILE_VORT_RAW}. Skipping.")
        return xr.open_dataset(FILE_VORT_RAW)

    log.info("Calculating relative vorticity (500 hPa and 850 hPa)...")

    dim_level = next(
        d for d in ["pressure_level", "level", "plev"] if d in ds.dims
    )
    dim_time = next(
        d for d in ["valid_time", "time"] if d in ds.dims or d in ds.coords
    )

    vort_list = []
    for level in [500, 850]:
        log.info(f"  {level} hPa...")
        ds_l = ds.sel({dim_level: level}).metpy.parse_cf()

        u = ds_l["u"].metpy.quantify()
        v = ds_l["v"].metpy.quantify()

        vor = mpcalc.vorticity(u, v).metpy.dequantify()

        if "metpy_crs" in vor.coords:
            vor = vor.drop_vars("metpy_crs")

        vor = vor.assign_coords({dim_level: level}).expand_dims(dim_level)
        vort_list.append(vor)

    vort_combined = xr.concat(vort_list, dim=dim_level)
    vort_combined.name = "vorticity"
    vort_combined.attrs["long_name"] = "Relative vorticity"
    vort_combined.attrs["units"] = "s-1"

    ds_vort = vort_combined.to_dataset()

    if dim_time != "time":
        ds_vort = ds_vort.rename({dim_time: "time"})

    # CDO requires time as the first dimension
    current_dims = list(ds_vort["vorticity"].dims)
    target_dims = ["time"] + [d for d in current_dims if d != "time"]
    log.info(f"  Reordering dimensions: {current_dims} → {target_dims}")
    ds_vort = ds_vort.transpose(*target_dims)

    ds_vort.to_netcdf(FILE_VORT_RAW)
    log.info(f"Raw vorticity saved: {FILE_VORT_RAW}")
    return ds_vort


# Monthly climatology

def _run_cdo(cmd: List[str]) -> None:
    """Run a CDO command and raise RuntimeError if it fails."""
    log.info(f"  CDO: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"CDO failed:\n{result.stderr}")


def calculate_cdo_climatology(input_file: Path, output_file: Path) -> None:
    """Calculate the 12-month climatology with CDO ymonmean."""
    if output_file.exists():
        log.info(f"Climatology already exists: {output_file}. Skipping.")
        return

    log.info(
        f"Calculating climatology (ymonmean): "
        f"{input_file.name} → {output_file.name}"
    )
    _run_cdo(["cdo", "ymonmean", str(input_file), str(output_file)])
    log.info(f"Climatology saved: {output_file}")


# Run processing

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info(f"ERA5 Climatology — period {START_YEAR}–{END_YEAR} [{CLIM_PERIOD}]")
    log.info("=" * 60)

    # Step 1: download monthly means
    log.info("\nStep 1/5: Downloading ERA5 monthly means...")
    annual_files = download_monthly_means()

    # Step 2: concatenate annual files
    log.info("\nStep 2/5: Concatenating annual files...")
    ds = concatenate_files(annual_files)
    log.info(f"  Dims: {dict(ds.dims)} | Vars: {list(ds.data_vars)}")

    # Step 3: extract 500 hPa geopotential
    log.info("\nStep 3/5: Extracting geopotential 500 hPa...")
    extract_gz500(ds)

    # Step 4: calculate relative vorticity
    log.info("\nStep 4/5: Calculating relative vorticity...")
    calculate_vorticity(ds)

    ds.close()

    # Step 5: calculate monthly climatologies
    log.info("\nStep 5/5: Calculating climatologies with CDO (ymonmean)...")
    calculate_cdo_climatology(FILE_GZ_500, FILE_CLIM_GZ)
    calculate_cdo_climatology(FILE_VORT_RAW, FILE_CLIM_VORT)

    # Verify generated files
    log.info("\n" + "=" * 60)
    log.info("Done! Files generated:")
    for file in [FILE_CLIM_GZ, FILE_CLIM_VORT]:
        ds_check = xr.open_dataset(file)
        n_months = len(ds_check.time) if "time" in ds_check.dims else "?"
        vars_str = ", ".join(
            f"{v} ({ds_check[v].attrs.get('units', '?')})"
            for v in ds_check.data_vars
        )
        log.info(f"  {file.name}: {n_months} timesteps | vars: {vars_str}")
        ds_check.close()
    log.info("=" * 60)


if __name__ == "__main__":
    main()
