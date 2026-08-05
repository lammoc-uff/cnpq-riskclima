import numpy as np
import xarray as xr

from riskclima_xhwi.features.humidity import (
    dewpoint_to_relative_humidity,
    specific_to_relative_humidity_standard_pressure,
)


class TestDewpointToRelativeHumidity:
    def test_equal_dewpoint_and_temperature_produce_saturation(self) -> None:
        temperature = xr.DataArray([280.0, 300.0], dims="time")

        result = dewpoint_to_relative_humidity(temperature, temperature)

        np.testing.assert_allclose(result.values, [100.0, 100.0])
        assert result.name == "hurs"
        assert result.attrs["units"] == "%"

    def test_clips_supersaturation(self) -> None:
        result = dewpoint_to_relative_humidity(
            xr.DataArray([290.0]), xr.DataArray([280.0]), clip=True
        )

        np.testing.assert_allclose(result.values, [100.0])


class TestSpecificToRelativeHumidity:
    def test_matches_standard_pressure_bolton_formula(self) -> None:
        huss = xr.DataArray([0.01], dims="time")
        tas = xr.DataArray([300.0], dims="time", attrs={"units": "K"})

        result = specific_to_relative_humidity_standard_pressure(huss, tas, pressure_pa=101325.0)

        vapor_pressure = (0.01 * 101325.0) / (0.622 + (1.0 - 0.622) * 0.01)
        saturation_pressure = 611.2 * np.exp((17.67 * 26.85) / (26.85 + 243.5))
        np.testing.assert_allclose(result, [100.0 * vapor_pressure / saturation_pressure])
        assert result.attrs["assumed_pressure_pa"] == 101325.0
