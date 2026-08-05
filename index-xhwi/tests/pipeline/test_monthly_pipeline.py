import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.config.settings import ERA5Settings
from riskclima_xhwi.pipeline.monthly_pipeline import compute_monthly_xhwi_torch


class TestComputeMonthlyXHWI:
    def test_processes_prepared_inputs_with_generic_pipeline(self) -> None:
        time = np.arange(
            np.datetime64("2000-01-01T00"),
            np.datetime64("2000-01-02T00"),
            np.timedelta64(1, "h"),
        )
        coords = {"time": time, "lat": [-22.0], "lon": [-43.0]}
        tas = xr.DataArray(
            np.full((24, 1, 1), 40.0, dtype=np.float32),
            dims=("time", "lat", "lon"),
            coords=coords,
        )
        hurs = xr.full_like(tas, 50.0)
        calibration = xr.DataArray(
            np.array([30.0, 35.0], dtype=np.float32).reshape(2, 1, 1),
            dims=("calibration_time", "lat", "lon"),
            coords={
                "calibration_time": np.array(["1961-01-01", "1962-01-01"], dtype="datetime64[D]"),
                "lat": [-22.0],
                "lon": [-43.0],
            },
        )

        result = compute_monthly_xhwi_torch(
            tas,
            hurs,
            calibration,
            months=[1],
            settings=ERA5Settings(_env_file=None, device="cpu"),
            source_label="synthetic",
        )

        assert result["xhwi_monthly_accumulated"].shape == (1, 1, 1)
        assert result.attrs["source_id"] == "era5"
        assert result.attrs["processed_calendar_months"] == "01"

    def test_transposes_inputs_before_processing(self) -> None:
        tas, hurs, calibration = self._prepared_inputs()

        result = compute_monthly_xhwi_torch(
            tas.transpose("lon", "time", "lat"),
            hurs.transpose("lat", "lon", "time"),
            calibration.transpose("lon", "lat", "calibration_time"),
            months=[1],
            settings=ERA5Settings(_env_file=None, device="cpu"),
            source_label="synthetic",
        )

        assert result["xhwi_monthly_accumulated"].dims == ("time", "lat", "lon")

    @pytest.mark.parametrize("coordinate", ["lat", "lon"])
    def test_rejects_calibration_on_a_different_grid(self, coordinate: str) -> None:
        tas, hurs, calibration = self._prepared_inputs()
        calibration = calibration.assign_coords({coordinate: calibration[coordinate] + 0.5})

        with pytest.raises(ValueError, match=f"calibration {coordinate} coordinates"):
            compute_monthly_xhwi_torch(
                tas,
                hurs,
                calibration,
                months=[1],
                settings=ERA5Settings(_env_file=None, device="cpu"),
                source_label="synthetic",
            )

    def test_rejects_application_inputs_with_different_times(self) -> None:
        tas, hurs, calibration = self._prepared_inputs()
        hurs = hurs.assign_coords(time=hurs["time"] + np.timedelta64(1, "h"))

        with pytest.raises(ValueError, match="tas and hurs coordinates"):
            compute_monthly_xhwi_torch(
                tas,
                hurs,
                calibration,
                months=[1],
                settings=ERA5Settings(_env_file=None, device="cpu"),
                source_label="synthetic",
            )

    @staticmethod
    def _prepared_inputs() -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
        time = np.arange(
            np.datetime64("2000-01-01T00"),
            np.datetime64("2000-01-02T00"),
            np.timedelta64(1, "h"),
        )
        coords = {"time": time, "lat": [-22.0], "lon": [-43.0]}
        tas = xr.DataArray(
            np.full((24, 1, 1), 40.0, dtype=np.float32),
            dims=("time", "lat", "lon"),
            coords=coords,
        )
        calibration = xr.DataArray(
            np.array([30.0, 35.0], dtype=np.float32).reshape(2, 1, 1),
            dims=("calibration_time", "lat", "lon"),
            coords={
                "calibration_time": np.array(["1961-01-01", "1962-01-01"], dtype="datetime64[D]"),
                "lat": [-22.0],
                "lon": [-43.0],
            },
        )
        return tas, xr.full_like(tas, 50.0), calibration
