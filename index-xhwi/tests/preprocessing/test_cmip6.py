import numpy as np
import xarray as xr

from riskclima_xhwi.config.settings import InterpolationMethod
from riskclima_xhwi.preprocessing.cmip6 import clean_cmip6_dims, interpolate_to_hourly


class TestCleanCMIP6Dims:
    def test_keeps_only_core_dimensions_and_sorts_coordinates(self) -> None:
        ds = xr.Dataset(
            {
                "tas": (("time", "lat", "lon"), np.ones((2, 2, 1))),
                "lat_bounds": (("lat", "bounds"), np.ones((2, 2))),
            },
            coords={
                "time": np.array(["2000-01-01T03", "2000-01-01T00"], dtype="datetime64[h]"),
                "lat": [1.0, 0.0],
                "lon": [2.0],
                "height": 2.0,
                "bounds": [0, 1],
            },
        )

        result = clean_cmip6_dims(ds)

        assert list(result.data_vars) == ["tas"]
        assert set(result.coords) == {"time", "lat", "lon"}
        np.testing.assert_array_equal(result["lat"], [0.0, 1.0])
        assert result["time"].values[0] == np.datetime64("2000-01-01T00")


class TestInterpolateToHourly:
    def test_sorts_then_linearly_interpolates(self) -> None:
        ds = xr.Dataset(
            {"tas": ("time", [6.0, 0.0])},
            coords={"time": np.array(["2000-01-01T03", "2000-01-01T00"], dtype="datetime64[h]")},
        )

        result = interpolate_to_hourly(ds, "1h", InterpolationMethod.LINEAR)

        np.testing.assert_allclose(result["tas"], [0.0, 2.0, 4.0, 6.0])
