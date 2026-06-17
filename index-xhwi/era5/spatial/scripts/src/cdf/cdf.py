import numpy as np
import xarray as xr
from scipy.interpolate import interp1d


def _sort_and_cdf_np(a_1d: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Sort a 1D array and compute its empirical cumulative distribution function (CDF).

    Parameters
    ----------
    a_1d : np.ndarray
        1D array (e.g., time series for a single grid point).

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        Sorted values and corresponding CDF probabilities.
    """
    a = np.asarray(a_1d)
    mask = np.isfinite(a)

    if not mask.any():
        n = a.size
        return np.full(n, np.nan), np.full(n, np.nan)

    valid = a[mask]
    sorted_valid = np.sort(valid)
    n = sorted_valid.size
    cdf_valid = np.arange(1, n + 1) / n

    # Fill with NaNs to keep same shape
    sorted_full = np.full_like(a, np.nan, dtype=sorted_valid.dtype)
    cdf_full = np.full_like(a, np.nan, dtype=float)

    sorted_full[mask] = sorted_valid
    cdf_full[mask] = cdf_valid

    return sorted_full, cdf_full


def compute_sorted_and_cdf(da: xr.DataArray, dim: str = "time") -> tuple[xr.DataArray, xr.DataArray]:
    """
    Compute sorted values and CDF along the selected dimension for each grid point.

    Parameters
    ----------
    da : xarray.DataArray
        DataArray containing the variable to process (e.g., temperature).
        Must include the selected dimension.

    Returns
    -------
    tuple[xr.DataArray, xr.DataArray]
        - Sorted values along the selected dimension.
        - CDF values along the selected dimension.
    """
    sorted_vals, cdf_vals = xr.apply_ufunc(
        _sort_and_cdf_np,
        da,
        input_core_dims=[[dim]],
        output_core_dims=[[dim], [dim]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[da.dtype, float],
    )

    sorted_vals.name = "sorted_temp"
    cdf_vals.name = "cdf"

    return sorted_vals, cdf_vals


def _interp_cdf_np(v_series, sorted_series, cdf_series):
    """
    Interpolate the CDF value of a validation temperature series given
    calibration sorted temperatures and corresponding CDF values.

    Parameters
    ----------
    v_series : np.ndarray
        Validation temperature values (1D).
    sorted_series : np.ndarray
        Sorted calibration temperature values (1D).
    cdf_series : np.ndarray
        CDF values corresponding to sorted_series.

    Returns
    -------
    np.ndarray
        Interpolated CDF probabilities for validation values.
    """
    v = np.asarray(v_series)
    x = np.asarray(sorted_series)
    y = np.asarray(cdf_series)

    # Handle missing data
    if not np.isfinite(x).any() or not np.isfinite(v).any():
        return np.full_like(v, np.nan, dtype=float)

    mask_xy = np.isfinite(x) & np.isfinite(y)
    if mask_xy.sum() < 2:
        return np.full_like(v, np.nan, dtype=float)

    f = interp1d(
        x[mask_xy],
        y[mask_xy],
        bounds_error=False,
        fill_value=(0.0, 1.0),
        assume_sorted=True,
    )
    out = f(v)
    out[~np.isfinite(v)] = np.nan
    return out


def match_cdf(
    validation_t2m: xr.DataArray,
    sorted_temp: xr.DataArray,
    cdf_vals: xr.DataArray,
    validation_dim: str = "time",
    calibration_dim: str = "calibration_time",
) -> xr.DataArray:
    """
    Match validation temperatures to their cumulative probability
    based on the calibration sorted temperature distribution.

    Parameters
    ----------
    validation_t2m : xr.DataArray
        Validation temperature time series.
    sorted_temp : xr.DataArray
        Sorted calibration temperatures along the calibration dimension.
    cdf_vals : xr.DataArray
        CDF probabilities corresponding to sorted_temp.

    Returns
    -------
    xr.DataArray
        DataArray of the same shape as validation_t2m containing
        interpolated CDF probabilities (Target variable).
    """
    target = xr.apply_ufunc(
        _interp_cdf_np,
        validation_t2m,
        sorted_temp,
        cdf_vals,
        input_core_dims=[[validation_dim], [calibration_dim], [calibration_dim]],
        output_core_dims=[[validation_dim]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[float],
    )
    target.name = "Target"
    return target
