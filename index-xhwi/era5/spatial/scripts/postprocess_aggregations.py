#!/usr/bin/env python3
"""
Postprocess XHWI results:
  - Reads hourly Zarr files (1–12 months)
  - Computes daily and monthly aggregations
  - Concatenates monthly results into a single NetCDF file
  - Concatenates daily results into a single NetCDF file
"""

from pathlib import Path
import gc
import dask
import xarray as xr
from dask.diagnostics import ProgressBar

from src.features.aggregations import monthly_indicators
from src.utils.encoding import clear_chunks_encoding
from src.config.settings import (
    RESULT_ROOT,
    RESULT_XHWI,
    RESULT_MONTHLY_COMBINED,
    RESULT_DAILY_COMBINED,
    CHUNKS,
    DAILY_VARIABLE,
)
from src.utils.logger import get_logger

logger = get_logger("postprocess_aggregations")

# boas configs dask para esse padrão de tarefa
dask.config.set({
                 "array.slicing.split_large_chunks": False,
                 "optimization.fuse.active": True,
                 })


def aggregate_single_month(month_idx: int, results_dir: Path):
    """
    Open a monthly Zarr (hourly resolution), compute daily and monthly aggregates.

    Returns
    -------
    tuple of (xr.Dataset | None, xr.Dataset | None)
        (daily_dataset, monthly_dataset)
    """
    path = Path(str(RESULT_XHWI).format(m=month_idx))

    if not path.exists():
        logger.warning(f"⚠️ File not found for month {month_idx}: {path}")
        return None, None

    logger.info(f"📂 Opening hourly dataset for month {month_idx}: {path}")
    # consolidated=True acelera muito a leitura de metadados
    ds_hourly = xr.open_zarr(path, consolidated=True).unify_chunks()

    logger.info(f"→ Aggregating daily and monthly indicators for month {month_idx}")
    ds_daily, ds_monthly = monthly_indicators(ds_hourly)
    logger.info(f"→ daily and monthly indicators for month {month_idx} aggregated")
    
    # cleaning memory
    del ds_hourly
    gc.collect()

    # defina chunk final só antes de gravar/concatenar
    # se quiser usar CHUNKS do seu settings, mantenha:
    ds_daily   = clear_chunks_encoding(ds_daily).chunk(CHUNKS)
    ds_monthly = clear_chunks_encoding(ds_monthly).chunk(CHUNKS)

    logger.info(f"✓ Month {month_idx}: monthly indicators ready (lazy).")
    return ds_daily, ds_monthly


def combine_monthly_to_netcdf(results_dir: Path, output_nc: Path):
    """
    Concatenate all monthly aggregated datasets (1–12) and export to NetCDF.
    """
    logger.info("🧩 Starting monthly concatenation and export")

    monthly_datasets = []
    for m in range(1, 13):
        _, ds_monthly = aggregate_single_month(m, results_dir)
        if ds_monthly is not None:
            monthly_datasets.append(ds_monthly)
        del ds_monthly
        gc.collect()

    if not monthly_datasets:
        logger.error("❌ No monthly datasets found. Aborting.")
        return

    logger.info("🔗 Concatenating all monthly datasets along time dimension")
    combined = xr.concat(monthly_datasets, dim="time").sortby("time")

    # streaming-friendly: chunks pequenos no tempo ajudam no to_netcdf
    # (se CHUNKS já definir isso, pode pular a linha abaixo)
    combined = combined.chunk({"time": 120})

    combined = clear_chunks_encoding(combined)

    logger.info(f"💾 Writing combined NetCDF to: {output_nc}")
    output_nc.parent.mkdir(parents=True, exist_ok=True)

    with ProgressBar():
        combined.to_netcdf(output_nc)  # compute ocorre aqui

    logger.info("✅ Postprocessing completed successfully.")

def combine_daily_to_netcdf(results_dir: Path, output_nc: Path):
    """
    Concatenate all daily aggregated datasets and export to NetCDF.
    """
    logger.info("🧩 Starting daily concatenation and export")

    daily_datasets = []
    for m in range(1, 13):
        ds_daily, _ = aggregate_single_month(m, results_dir)
        if ds_daily is not None:
            daily_datasets.append(ds_daily)
        del ds_daily
        gc.collect()

    if not daily_datasets:
        logger.error("❌ No daily datasets found. Aborting.")
        return

    logger.info("🔗 Concatenating all daily datasets along time dimension")
    combined = xr.concat(daily_datasets, dim="time").sortby("time")

    # streaming-friendly: chunks pequenos no tempo ajudam no to_netcdf
    # (se CHUNKS já definir isso, pode pular a linha abaixo)
    combined = combined.chunk({"time": 120})

    combined = clear_chunks_encoding(combined)

    logger.info(f"💾 Writing combined NetCDF to: {output_nc}")
    output_nc.parent.mkdir(parents=True, exist_ok=True)

    with ProgressBar():
        combined.to_netcdf(output_nc)  # compute works in here

    logger.info("✅ Postprocessing completed successfully.")


def monthly_main():
    logger.info("🚀 Starting postprocessing of XHWI results")

    results_dir = Path(RESULT_ROOT)
    output_nc = Path(RESULT_MONTHLY_COMBINED)

    combine_monthly_to_netcdf(results_dir, output_nc)

    logger.info(f"🏁 Final NetCDF written to: {output_nc}")


def daily_main():
    logger.info("🚀 Starting postprocessing of XHWI results")

    results_dir = Path(RESULT_ROOT)
    output_nc = Path(RESULT_DAILY_COMBINED)

    combine_daily_to_netcdf(results_dir, output_nc)

    logger.info(f"🏁 Final NetCDF written to: {output_nc}")


if __name__ == "__main__":
    if DAILY_VARIABLE:
        daily_main()

    else:
        monthly_main()
