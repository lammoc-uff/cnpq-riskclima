import gc

import numpy as np
import torch
import xarray as xr

from src.config.settings import DEVICE, DTYPE
from src.torch_ops.aggregations import torch_monthly_accumulated_xhwi
from src.torch_ops.cdf import torch_match_cdf_linear
from src.torch_ops.xhwi import torch_heatwave_index


def load_block_np(da: xr.DataArray, lat_slice: slice, lon_slice: slice) -> np.ndarray:
    return da.isel(lat=lat_slice, lon=lon_slice).load().values.astype("float32")


def process_month_block_torch(
    tas_c_month: xr.DataArray,
    hurs_month: xr.DataArray,
    tasmax_calibration_month: xr.DataArray,
    lat_slice: slice,
    lon_slice: slice,
) -> tuple[np.ndarray, np.ndarray]:
    tas_np = load_block_np(tas_c_month, lat_slice, lon_slice)
    hurs_np = load_block_np(hurs_month, lat_slice, lon_slice)
    tasmax_np = load_block_np(tasmax_calibration_month, lat_slice, lon_slice)

    tas_t = torch.as_tensor(tas_np, dtype=DTYPE, device=DEVICE)
    hurs_t = torch.as_tensor(hurs_np, dtype=DTYPE, device=DEVICE)
    tasmax_t = torch.as_tensor(tasmax_np, dtype=DTYPE, device=DEVICE)

    target_t = torch_match_cdf_linear(tas_t, tasmax_t)
    xhwi_t = torch_heatwave_index(tas_c=tas_t, hurs=hurs_t, target=target_t)
    monthly_np, monthly_time = torch_monthly_accumulated_xhwi(xhwi_t, tas_c_month["time"])

    del tas_t, hurs_t, tasmax_t, target_t, xhwi_t
    if DEVICE.type == "cuda":
        torch.cuda.empty_cache()
    gc.collect()
    return monthly_np, monthly_time
