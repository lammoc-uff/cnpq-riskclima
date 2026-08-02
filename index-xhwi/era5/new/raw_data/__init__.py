"""Input and intermediate data paths for the ERA5 XHWI workflow."""

from pathlib import Path

RAW_DATA_DIR = Path(__file__).resolve().parent
CALIBRATION_FILE = RAW_DATA_DIR / "xhwi_era5_calib_t2m_max_1961-1990.nc"
