from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.preprocessing.era5 import (
    get_cdsapi_key,
    kelvin_to_celsius,
    standardize_era5_dims,
)


class TestEra5Preprocessing:
    def test_loads_cdsapi_key_from_current_dotenv(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / ".env").write_text("CDSAPI_KEY=dotenv-secret\n", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CDSAPI_KEY", raising=False)

        assert get_cdsapi_key() == "dotenv-secret"

    def test_standardizes_and_sorts_dimensions(self) -> None:
        data = xr.DataArray(
            np.zeros((2, 2, 1)),
            dims=("valid_time", "latitude", "longitude"),
            coords={
                "valid_time": np.array(["2001-01-02", "2001-01-01"], dtype="datetime64[D]"),
                "latitude": [1.0, 0.0],
                "longitude": [2.0],
            },
        )

        result = standardize_era5_dims(data)

        assert result.dims == ("time", "lat", "lon")
        np.testing.assert_array_equal(result["lat"], [0.0, 1.0])

    def test_converts_kelvin_from_units_metadata(self) -> None:
        data = xr.DataArray([273.15], attrs={"units": "K"})

        result = kelvin_to_celsius(data)

        np.testing.assert_allclose(result, [0.0], atol=1e-6)
        assert result.attrs["units"] == "degC"
