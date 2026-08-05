import gc

import numpy as np
import torch
import xarray as xr
from numpy.typing import NDArray

from riskclima_xhwi.config.settings import XHWISettings
from riskclima_xhwi.torch_ops.aggregations import torch_monthly_accumulated_xhwi
from riskclima_xhwi.torch_ops.cdf import torch_match_cdf_linear
from riskclima_xhwi.torch_ops.xhwi import torch_heatwave_index


def load_block_np(
    da: xr.DataArray,
    lat_slice: slice,
    lon_slice: slice,
    dtype: np.dtype[np.float32] | np.dtype[np.float64],
) -> NDArray[np.float32] | NDArray[np.float64]:
    """Load a spatial data-array block using the configured NumPy dtype."""
    values = da.isel(lat=lat_slice, lon=lon_slice).load().values
    if dtype == np.dtype("float32"):
        return values.astype(np.float32)
    return values.astype(np.float64)


def process_month_block_torch(
    tas_c_month: xr.DataArray,
    hurs_month: xr.DataArray,
    tasmax_calibration_month: xr.DataArray,
    lat_slice: slice,
    lon_slice: slice,
    settings: XHWISettings,
) -> tuple[NDArray[np.float32] | NDArray[np.float64], NDArray[np.generic]]:
    """Process one spatial block through CDF, XHWI, and aggregation."""
    device = settings.resolve_device()
    tas_t = torch.as_tensor(
        load_block_np(tas_c_month, lat_slice, lon_slice, settings.numpy_type),
        dtype=settings.torch_type,
        device=device,
    )
    hurs_t = torch.as_tensor(
        load_block_np(hurs_month, lat_slice, lon_slice, settings.numpy_type),
        dtype=settings.torch_type,
        device=device,
    )
    tasmax_t = torch.as_tensor(
        load_block_np(tasmax_calibration_month, lat_slice, lon_slice, settings.numpy_type),
        dtype=settings.torch_type,
        device=device,
    )
    target_t = torch_match_cdf_linear(tas_t, tasmax_t)
    xhwi_t = torch_heatwave_index(
        tas_c=tas_t,
        hurs=hurs_t,
        target=target_t,
        xhwi_minimum=settings.xhwi_minimum,
    )
    monthly_np, monthly_time = torch_monthly_accumulated_xhwi(xhwi_t, tas_c_month["time"])
    del tas_t, hurs_t, tasmax_t, target_t, xhwi_t
    if device.type == "cuda":
        torch.cuda.empty_cache()
    gc.collect()
    return monthly_np, monthly_time
