from pathlib import Path

# I/O paths
PATH = Path(__file__).resolve().parents[4]
RAW = PATH / "raw_data"
CALIB_PATH = RAW / "temp_max_Brazil_1961-1990.nc"
ZARR_R = RAW / "r_prev_STARTYEAR-ENDYEAR" / "zarr_store_humidity_br_STARTYEAR-ENDYEAR"
ZARR_T2M = RAW / "t2m_prev_STARTYEAR-ENDYEAR" / "zarr_store_t2m_br_STARTYEAR-ENDYEAR"

RESULT_ROOT = PATH / "spatial" / "results"
RESULT_XHWI = RESULT_ROOT / "xhwi_era5_STARTYEAR-ENDYEAR_br_month_{m}_zarr_store.zarr"  # Calculated Index Hourly
RESULT_XHWI_DIARY_INDPROD = RESULT_ROOT / "xhwi_era5_STARTYEAR-ENDYEAR_br_diary_ind_prod_{m}_zarr_store.zarr"
RESULT_XHWI_MONTH_INDPROD = RESULT_ROOT / "xhwi_era5_STARTYEAR-ENDYEAR_br_month_ind_prod_{m}_zarr_store.zarr"

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
