import numpy as np
import xarray as xr


def dewpoint_to_relative_humidity(
    d2m: xr.DataArray,
    t2m: xr.DataArray,
    *,
    clip: bool = True,
) -> xr.DataArray:
    """Calculate relative humidity from two-meter temperatures.

    Parameters
    ----------
    d2m
        Two-meter dewpoint temperature in kelvin.
    t2m
        Two-meter air temperature in kelvin.
    clip
        Whether to constrain relative humidity to 0 through 100 percent.

    Returns
    -------
    xarray.DataArray
        Relative humidity in percent.
    """
    exponent = (17.502 * (d2m - 273.16) / (d2m - 32.19)) - (17.502 * (t2m - 273.16) / (t2m - 32.19))
    rh = 100.0 * np.e**exponent
    if clip:
        rh = rh.clip(min=0.0, max=100.0)
    rh.name = "hurs"
    rh.attrs.update(
        {
            "long_name": (
                "Relative humidity calculated from 2 m dewpoint temperature and 2 m temperature"
            ),
            "units": "%",
            "formula": (
                "RH = 100 * exp(17.502*(d2m-273.16)/(d2m-32.19) - 17.502*(t2m-273.16)/(t2m-32.19))"
            ),
            "source_variables": "d2m, t2m",
        }
    )
    return rh


def specific_to_relative_humidity_standard_pressure(
    huss: xr.DataArray,
    tas: xr.DataArray,
    *,
    pressure_pa: float,
    clip: bool = True,
) -> xr.DataArray:
    """Convert specific humidity to relative humidity using Bolton saturation pressure.

    Parameters
    ----------
    huss
        Specific humidity as a mass fraction.
    tas
        Air temperature in kelvin or degrees Celsius.
    pressure_pa
        Assumed constant surface pressure in pascals.
    clip
        Whether to constrain relative humidity to 0 through 100 percent.

    Returns
    -------
    xarray.DataArray
        Relative humidity in percent.
    """
    epsilon = 0.622
    units = str(tas.attrs.get("units", "")).lower()
    tas_c = tas - 273.15 if units in {"k", "kelvin"} else tas
    vapor_pressure = (huss * pressure_pa) / (epsilon + (1.0 - epsilon) * huss)
    saturation_pressure = 611.2 * np.e ** ((17.67 * tas_c) / (tas_c + 243.5))
    hurs = 100.0 * vapor_pressure / saturation_pressure
    if clip:
        hurs = hurs.clip(min=0.0, max=100.0)
    hurs.name = "hurs"
    hurs.attrs.update(
        {
            "standard_name": "relative_humidity",
            "long_name": "Relative humidity",
            "units": "%",
            "description": (
                "Relative humidity computed from specific humidity and air temperature "
                f"assuming constant surface pressure p = {pressure_pa:g} Pa. Saturation "
                "vapor pressure uses Bolton (1980)."
            ),
            "assumed_pressure_pa": pressure_pa,
        }
    )
    return hurs
