import numpy as np
import xarray as xr


def dewpoint_to_relative_humidity(
    d2m: xr.DataArray,
    t2m: xr.DataArray,
    clip: bool = True,
) -> xr.DataArray:
    """Calculate relative humidity (%) from 2 m dewpoint and 2 m temperature."""
    rh = 100.0 * np.exp(
        (17.502 * (d2m - 273.16) / (d2m - 32.19))
        - (17.502 * (t2m - 273.16) / (t2m - 32.19))
    )

    if clip:
        rh = rh.clip(min=0.0, max=100.0)

    rh.name = "hurs"
    rh.attrs.update(
        {
            "long_name": "Relative humidity calculated from 2 m dewpoint temperature and 2 m temperature",
            "units": "%",
            "formula": "RH = 100 * exp(17.502*(d2m-273.16)/(d2m-32.19) - 17.502*(t2m-273.16)/(t2m-32.19))",
            "source_variables": "d2m, t2m",
        }
    )
    return rh
