from pathlib import Path

# I/O paths
PATH = Path(__file__).resolve().parents[4]
RAW = PATH / "raw_data"
PERIOD_NAME = "STARTYEAR_ENDYEAR"
CALIB_PATH = RAW / "temp_max_Brazil_1961-1990.nc"
ZARR_R = RAW / f"r_prev_{PERIOD_NAME}" / f"zarr_store_humidity_br_{PERIOD_NAME}"
ZARR_T2M = RAW / f"t2m_prev_{PERIOD_NAME}" / f"zarr_store_t2m_br_{PERIOD_NAME}"

RESULT_ROOT = PATH / "spatial" / "results"
RESULT_XHWI = RESULT_ROOT / f"xhwi_era5_{PERIOD_NAME}_br_month_{{m}}_zarr_store.zarr"  # Calculated Index Hourly
RESULT_XHWI_DIARY_INDPROD = RESULT_ROOT / f"xhwi_era5_{PERIOD_NAME}_br_diary_ind_prod_{{m}}_zarr_store.zarr"
RESULT_XHWI_MONTH_INDPROD = RESULT_ROOT / f"xhwi_era5_{PERIOD_NAME}_br_month_ind_prod_{{m}}_zarr_store.zarr"
RESULT_MONTHLY_COMBINED = RESULT_ROOT / f"xhwi_era5_{PERIOD_NAME}_br_monthly_combined.nc"
RESULT_DAILY_COMBINED = RESULT_ROOT / f"xhwi_era5_{PERIOD_NAME}_br_daily_combined.nc"

# Dask / compute
DASK_SCHEDULER = "threads"   # "threads" | "processes"
DASK_N_WORKERS = None        # None = auto
DASK_THREADS_PER_WORKER = 1

# Chunking policy (tune for your machine)
CHUNKS = {"time": -1, "latitude": 10, "longitude": 10}

# Pipeline behavior
OVERWRITE = True
WRITE_CONSOLIDATED = True

# Coordinates
FIX_LONGITUDE_TO_180 = True
TARGET_CAL_VAR = "VAR_2T"  # name in calibration dataset

# For postprocess --> CHANGE IT HERE
"""
Use DAILY_VARIABLE == False for monthly ind_prod or 
DAILY_VARIABLE == True for daily ind_prod
"""
DAILY_VARIABLE = True
