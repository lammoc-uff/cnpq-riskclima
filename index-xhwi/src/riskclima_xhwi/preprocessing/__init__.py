"""Input data preprocessing."""

from riskclima_xhwi.preprocessing.era5 import (
    kelvin_to_celsius,
    open_era5_zarr,
    open_saved_tasmax_calibration,
    standardize_era5_dims,
)

__all__ = [
    "kelvin_to_celsius",
    "open_era5_zarr",
    "open_saved_tasmax_calibration",
    "standardize_era5_dims",
]
from riskclima_xhwi.preprocessing.cmip6 import clean_cmip6_dims, interpolate_to_hourly

__all__ = ["clean_cmip6_dims", "interpolate_to_hourly"]
