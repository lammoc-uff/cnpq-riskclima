import torch


def torch_match_cdf_linear(
    tas_hourly_c: torch.Tensor,
    tasmax_calibration_c: torch.Tensor,
) -> torch.Tensor:
    """Match hourly temperature to an empirical calibration CDF.

    Parameters
    ----------
    tas_hourly_c
        Hourly temperature with dimensions time, latitude, and longitude.
    tasmax_calibration_c
        Calibration daily maximum temperature on the same spatial grid.

    Returns
    -------
    torch.Tensor
        Empirical cumulative probabilities interpolated linearly.
    """
    time_size, y_size, x_size = tas_hourly_c.shape
    values = tas_hourly_c.reshape(time_size, -1).transpose(0, 1).contiguous()
    calibration = (
        tasmax_calibration_c.reshape(tasmax_calibration_c.shape[0], -1).transpose(0, 1).contiguous()
    )
    finite_cal = torch.isfinite(calibration)
    n_valid = finite_cal.sum(dim=1)
    calibration_sorted = calibration.masked_fill(~finite_cal, float("inf")).sort(dim=1).values
    finite_values = torch.isfinite(values)
    safe_values = values.masked_fill(~finite_values, 0.0)
    idx_right = torch.searchsorted(calibration_sorted, safe_values, right=False)
    max_idx = torch.clamp(n_valid - 1, min=0).unsqueeze(1)
    idx1 = torch.minimum(idx_right, max_idx).long()
    idx0 = torch.clamp(idx1 - 1, min=0).long()
    x0 = calibration_sorted.gather(1, idx0)
    x1 = calibration_sorted.gather(1, idx1)
    n_valid_f = n_valid.clamp(min=1).unsqueeze(1).to(values.dtype)
    y0 = (idx0.to(values.dtype) + 1.0) / n_valid_f
    y1 = (idx1.to(values.dtype) + 1.0) / n_valid_f
    denom = x1 - x0
    frac = torch.where(
        torch.abs(denom) > 0,
        (safe_values - x0) / denom,
        torch.zeros_like(safe_values),
    )
    target = y0 + frac * (y1 - y0)
    first = calibration_sorted[:, 0].unsqueeze(1)
    last = calibration_sorted.gather(1, max_idx.long())
    target = torch.where(safe_values < first, torch.zeros_like(target), target)
    target = torch.where(safe_values >= last, torch.ones_like(target), target)
    target = torch.where((n_valid < 2).unsqueeze(1), torch.full_like(target, float("nan")), target)
    target = torch.where(finite_values, target, torch.full_like(target, float("nan")))
    target = torch.clamp(target, min=0.0, max=1.0)
    return target.transpose(0, 1).reshape(time_size, y_size, x_size)
