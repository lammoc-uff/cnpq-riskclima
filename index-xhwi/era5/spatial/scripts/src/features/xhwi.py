import xarray as xr
import numpy as np


def heatwave_index(ds: xr.Dataset) -> xr.Dataset:
    """
    Compute the Extreme Heatwave Index (XHWI) based on temperature and humidity.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset containing at least the variables:
        - 'Target' : cumulative probability from calibration CDF.
        - 't2m'    : 2-meter air temperature (°C).
        - 'r'      : relative humidity (%).

    Returns
    -------
    xarray.Dataset
        Dataset with added variables:
        - 'Target100' : Target * 100
        - 'tpe'       : exceedance over 95th percentile threshold
        - 'Coef'      : exponential coefficient term
        - 'XHWI'      : final heatwave intensity index

    Notes
    -----
    Formula based on empirical relationships between temperature,
    humidity, and extreme heatwave probability.
    """

    ds = ds.copy()

    # Step 1: Transform target (0–1) to percentage scale
    ds["Target100"] = ds["Target"] * 100

    # Step 2: Temperature exceedance above 95th percentile
    ds["tpe"] = (ds["Target100"] - 95).clip(min=0)

    # Step 3: Coefficient term
    ds["Coef"] = (np.exp(ds["tpe"]) * ds["r"]) / 1000.0

    # Step 4: Heatwave intensity
    ds["XHWI"] = (ds["Coef"] - 0.001) / 14.84

    # Step 5: Apply masks (physical thresholds)
    ds["XHWI"] = ds["XHWI"].where(ds["tpe"] > 0, 0)
    ds["XHWI"] = ds["XHWI"].where(ds["t2m"] > 32, 0)
    ds["XHWI"] = ds["XHWI"].where(ds["XHWI"] > 0.001, 0)

    return ds
