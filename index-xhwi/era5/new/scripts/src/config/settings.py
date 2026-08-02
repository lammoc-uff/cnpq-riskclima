from pathlib import Path

import torch

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
OUTPUT_DIR = PROJECT_ROOT / "results"
MONTHLY_DIR = OUTPUT_DIR / "monthly"
MONTHLY_PARTS_DIR = MONTHLY_DIR / "parts"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
MONTHLY_PARTS_DIR.mkdir(parents=True, exist_ok=True)

# ERA5 ARCO/Zarr
ERA5_ZARR_PATH = (
    "https://arco.datastores.ecmwf.int/"
    "cadl-arco-geo-002/arco/reanalysis_era5_single_levels/sfc/geoChunked.zarr"
)
DATASET_ID = "ERA5_ARCO"
SOURCE = "era5"
VARIABLE_T2M = "t2m"
VARIABLE_D2M = "d2m"

# Calibration
CALIBRATION_PERIOD = ("1961-01-01", "1990-12-31")
CALIB_OUT = RAW_DATA_DIR / "xhwi_era5_calib_t2m_max_1961-1990.nc"

# Spatial domain
LAT_SLICE = slice(-70, 20)
LON_SLICE = slice(-120, -5)

# Output files
MONTHLY_OUTPUT_FILE = MONTHLY_DIR / "xhwi_era5_monthly_ind_prod.nc"

# Months to process. Edit this list to choose which calendar months run_months.py processes.
MONTHS_TO_RUN = list(range(1, 13))

# XHWI parameters
TEMPERATURE_THRESHOLD_C = 32.0
CDF_THRESHOLD_PERCENT = 95.0

# PyTorch / block processing
TORCH_DTYPE = "float32"
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
DTYPE = torch.float32 if TORCH_DTYPE == "float32" else torch.float64
LAT_BLOCK_SIZE = 64
LON_BLOCK_SIZE = 64
