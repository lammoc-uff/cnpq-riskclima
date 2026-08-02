import numpy as np
import torch
import xarray as xr


def month_keys_from_time(time_coord: xr.DataArray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    years = time_coord.dt.year.values.astype(np.int64)
    months = time_coord.dt.month.values.astype(np.int64)
    days = time_coord.dt.day.values.astype(np.int64)
    day_keys = years * 10000 + months * 100 + days
    month_keys = years * 100 + months
    return day_keys, month_keys, np.asarray(time_coord.values)


def torch_monthly_accumulated_xhwi(
    xhwi: torch.Tensor,
    time_coord: xr.DataArray,
) -> tuple[np.ndarray, np.ndarray]:
    """Aggregate hourly XHWI into monthly accumulated values on GPU."""
    day_keys, month_keys_by_time, time_values = month_keys_from_time(time_coord)
    unique_days = np.unique(day_keys)

    daily_values = []
    daily_month_keys = []

    for day_key in unique_days:
        idx_np = np.flatnonzero(day_keys == day_key)
        idx = torch.as_tensor(idx_np, device=xhwi.device, dtype=torch.long)
        xhwi_day = xhwi.index_select(0, idx)
        active_hours = (xhwi_day != 0).sum(dim=0).to(xhwi.dtype)
        daily_sum = xhwi_day.sum(dim=0)
        daily_values.append(active_hours * daily_sum)
        daily_month_keys.append(month_keys_by_time[idx_np[0]])

    if not daily_values:
        raise ValueError("No daily values were generated for this block.")

    daily_stack = torch.stack(daily_values, dim=0)
    daily_month_keys = np.asarray(daily_month_keys)
    unique_months = np.unique(daily_month_keys)

    monthly_values = []
    monthly_time_values = []
    for month_key in unique_months:
        day_idx_np = np.flatnonzero(daily_month_keys == month_key)
        day_idx = torch.as_tensor(day_idx_np, device=xhwi.device, dtype=torch.long)
        monthly_values.append(daily_stack.index_select(0, day_idx).sum(dim=0))
        first_time_idx = np.flatnonzero(month_keys_by_time == month_key)[0]
        monthly_time_values.append(time_values[first_time_idx])

    monthly = torch.stack(monthly_values, dim=0).detach().cpu().numpy().astype("float32")
    return monthly, np.asarray(monthly_time_values)
