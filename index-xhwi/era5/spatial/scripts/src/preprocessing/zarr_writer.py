import os
import glob
import xarray as xr
from dask.diagnostics import ProgressBar
from src.utils.logger import get_logger

def process_variable(
    input_dir: str,
    output_zarr: str,
    var_name: str,
    kelvin_to_celsius: bool = False,
    chunks: str | dict = "auto",
):
    """
    Process and merge multiple NetCDF files into a single Zarr store.

    Parameters
    ----------
    input_dir : str
        Directory containing NetCDF files.
    output_zarr : str
        Path to output Zarr store.
    var_name : str
        Variable name to process (e.g., 't2m' or 'r').
    kelvin_to_celsius : bool, optional
        Whether to convert variable from Kelvin to Celsius. Default is False.
    chunks : str or dict, optional
        Chunking scheme for Dask/xarray. Default is 'auto'.

    Notes
    -----
    - Files are appended sequentially along the 'time' dimension.
    - If `output_zarr` already exists, it will be overwritten on the first iteration.
    - A progress bar will be displayed during Zarr writes.
    """
    logger = get_logger("zarr_writer")

    # List files
    pattern = os.path.join(input_dir, f"era5_br_{var_name}_*.nc")
    files = sorted(glob.glob(pattern))
    if not files:
        logger.error(f"No NetCDF files found for pattern: {pattern}")
        raise FileNotFoundError(f"No files found for {var_name} in {input_dir}")

    logger.info(f"Found {len(files)} files for variable '{var_name}'.")
    logger.info(f"Output Zarr store: {output_zarr}")

    with ProgressBar():
        for i, file in enumerate(files):
            logger.info(f"[{i+1}/{len(files)}] Processing file: {os.path.basename(file)}")
            try:
                ds = xr.open_dataset(file, chunks=chunks)

                if kelvin_to_celsius and var_name in ds.variables:
                    ds[var_name] = ds[var_name] - 273.15
                    ds[var_name].attrs["units"] = "°C"

                mode = "w" if i == 0 else "a"
                ds.to_zarr(output_zarr, mode=mode, append_dim="time")

                logger.info(f"✓ File processed successfully: {os.path.basename(file)}")

            except Exception as e:
                logger.error(f"⚠️ Error processing file {file}: {e}")
                continue

    logger.info("✅ Zarr store creation completed successfully.")
