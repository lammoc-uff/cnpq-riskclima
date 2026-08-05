import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.config.settings import (
    CMIP6Settings,
    ERA5LandSettings,
    Settings,
)
from riskclima_xhwi.pipeline import data_access
from riskclima_xhwi.pipeline.data_access import (
    open_calibration_tasmax_from_t2m,
    open_cmip6_calibration,
    open_cmip6_hourly_inputs,
    open_era5_land_inputs,
)


class TestOpenCalibrationTasmaxFromT2m:
    def test_uses_nonoverlapping_24_hour_daily_maxima(self) -> None:
        time = np.arange(
            np.datetime64("1961-01-01T00"),
            np.datetime64("1961-01-03T00"),
            np.timedelta64(1, "h"),
        )
        temperature = xr.DataArray(
            np.arange(48, dtype=np.float32).reshape(48, 1, 1),
            dims=("time", "lat", "lon"),
            coords={"time": time, "lat": [0.0], "lon": [0.0]},
        )

        result = open_calibration_tasmax_from_t2m(temperature, Settings(_env_file=None))

        np.testing.assert_allclose(result[:, 0, 0], [23.0, 47.0])
        assert result.dims == ("calibration_time", "lat", "lon")


class TestERA5LandInputs:
    def test_opens_separate_stores_and_selects_application_period(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        time = np.array(
            ["2009-12-31", "2010-01-01", "2025-12-31", "2026-01-01"],
            dtype="datetime64[D]",
        )
        coords = {"time": time, "lat": [-22.0], "lon": [-43.0]}
        temperature = xr.Dataset(
            {"2m_temperature": (("time", "lat", "lon"), np.full((4, 1, 1), 300.0))},
            coords=coords,
        )
        dewpoint = xr.Dataset(
            {
                "2m_dewpoint_temperature": (
                    ("time", "lat", "lon"),
                    np.full((4, 1, 1), 290.0),
                )
            },
            coords=coords,
        )
        opened_paths: list[str] = []

        def open_store(
            path: str, chunks: str, *, consolidated: bool, timeout_seconds: float
        ) -> xr.Dataset:
            opened_paths.append(path)
            return temperature if path == "temperature" else dewpoint

        monkeypatch.setattr(data_access, "open_era5_zarr", open_store)
        settings = ERA5LandSettings(_env_file=None, dewpoint_zarr_url="dewpoint")

        tas, hurs = open_era5_land_inputs("temperature", settings)

        assert opened_paths == ["temperature", "dewpoint"]
        assert tas.sizes["time"] == 2
        assert hurs.sizes["time"] == 2

    def test_rejects_mismatched_temperature_and_dewpoint_coordinates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        temperature = xr.Dataset(
            {"2m_temperature": (("time", "lat", "lon"), [[[300.0]]])},
            coords={"time": [np.datetime64("2020-01-01")], "lat": [-22.0], "lon": [-43.0]},
        )
        dewpoint = xr.Dataset(
            {"2m_dewpoint_temperature": (("time", "lat", "lon"), [[[290.0]]])},
            coords={"time": [np.datetime64("2020-01-01")], "lat": [-21.0], "lon": [-43.0]},
        )

        def open_store(
            path: str, chunks: str, *, consolidated: bool, timeout_seconds: float
        ) -> xr.Dataset:
            return temperature if path == "temperature" else dewpoint

        monkeypatch.setattr(data_access, "open_era5_zarr", open_store)
        settings = ERA5LandSettings(_env_file=None, dewpoint_zarr_url="dewpoint")

        with pytest.raises(ValueError, match="temperature and dewpoint coordinates"):
            open_era5_land_inputs("temperature", settings)


class TestCMIP6Inputs:
    def test_uses_dedicated_calibration_time_chunk(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_chunks: dict[str, int] = {}
        dataset = xr.Dataset(
            {"tasmax": (("time", "lat", "lon"), np.full((1, 1, 1), 300.0))},
            coords={"time": [np.datetime64("1961-01-01")], "lat": [0.0], "lon": [0.0]},
        )
        dataset["tasmax"].attrs["units"] = "K"

        def open_store(path: object, chunks: dict[str, int], *, consolidated: bool) -> xr.Dataset:
            captured_chunks.update(chunks)
            return dataset

        monkeypatch.setattr(data_access, "open_clean_cmip6_zarr", open_store)
        settings = CMIP6Settings.model_validate(
            CMIP6Settings().model_dump() | {"calibration_time_chunk": 365}
        )

        open_cmip6_calibration(settings)

        assert captured_chunks["time"] == 365

    def test_uses_configured_variable_names_and_application_period(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        time = np.array(["2000-01-01T00", "2000-01-01T03"], dtype="datetime64[h]")
        coords = {"time": time, "lat": [0.0], "lon": [0.0]}
        temperature = xr.Dataset(
            {"air_temp": (("time", "lat", "lon"), np.array([[[300.0]], [[303.0]]]))},
            coords=coords,
        )
        temperature["air_temp"].attrs["units"] = "K"
        humidity = xr.Dataset(
            {"specific_humidity": (("time", "lat", "lon"), np.array([[[0.01]], [[0.013]]]))},
            coords=coords,
        )
        settings = CMIP6Settings.model_validate(
            CMIP6Settings().model_dump()
            | {
                "variable_tas": "air_temp",
                "variable_huss": "specific_humidity",
                "scenario_tas_template": "cmip6/{model}/{scenario}/air_temp/{grid}/{member}.zarr",
                "scenario_huss_template": (
                    "cmip6/{model}/{scenario}/specific_humidity/{grid}/{member}.zarr"
                ),
                "application_start": "2000-01-01T01",
                "application_end": "2000-01-01T02",
            }
        )

        def open_store(path: object, chunks: object = None, *, consolidated: bool) -> xr.Dataset:
            return temperature if "air_temp" in str(path) else humidity

        monkeypatch.setattr(data_access, "open_clean_cmip6_zarr", open_store)

        tas_c, hurs = open_cmip6_hourly_inputs("ssp245", settings)

        assert tas_c.sizes["time"] == 2
        assert hurs.sizes["time"] == 2
        np.testing.assert_allclose(tas_c[:, 0, 0], [27.85, 28.85])

    def test_interpolates_tas_and_huss_before_relative_humidity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        time = np.array(["2000-01-01T00", "2000-01-01T03"], dtype="datetime64[h]")
        coords = {"time": time, "lat": [0.0], "lon": [0.0]}
        tas = xr.Dataset(
            {"tas": (("time", "lat", "lon"), np.array([[[300.0]], [[303.0]]]))},
            coords=coords,
        )
        tas["tas"].attrs["units"] = "K"
        huss = xr.Dataset(
            {"huss": (("time", "lat", "lon"), np.array([[[0.01]], [[0.013]]]))},
            coords=coords,
        )

        def open_store(path: object, chunks: object = None, *, consolidated: bool) -> xr.Dataset:
            return tas if "/tas/" in str(path) else huss

        monkeypatch.setattr(data_access, "open_clean_cmip6_zarr", open_store)

        tas_c, hurs = open_cmip6_hourly_inputs(
            "ssp245", CMIP6Settings(_env_file=None, device="cpu")
        )

        assert tas_c.sizes["time"] == 4
        assert hurs.sizes["time"] == 4
        np.testing.assert_allclose(tas_c[:, 0, 0], [26.85, 27.85, 28.85, 29.85])

    def test_rejects_mismatched_tas_and_huss_coordinates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        time = np.array(["2000-01-01T00", "2000-01-01T03"], dtype="datetime64[h]")
        tas = xr.Dataset(
            {"tas": (("time", "lat", "lon"), np.full((2, 1, 1), 300.0))},
            coords={"time": time, "lat": [0.0], "lon": [0.0]},
        )
        huss = xr.Dataset(
            {"huss": (("time", "lat", "lon"), np.full((2, 1, 1), 0.01))},
            coords={"time": time, "lat": [1.0], "lon": [0.0]},
        )

        def open_store(path: object, chunks: object = None, *, consolidated: bool) -> xr.Dataset:
            return tas if "/tas/" in str(path) else huss

        monkeypatch.setattr(data_access, "open_clean_cmip6_zarr", open_store)

        with pytest.raises(ValueError, match="tas and huss coordinates"):
            open_cmip6_hourly_inputs("ssp245", CMIP6Settings(_env_file=None, device="cpu"))
