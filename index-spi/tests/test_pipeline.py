from pathlib import Path

import numpy as np
import numpy.typing as npt
import pytest
import xarray as xr
from dask.array import Array

import riskclima_spi.cmip6 as cmip6_module
import riskclima_spi.era5 as era5_module
from riskclima_spi.cmip6 import prepare_cmip6_monthly_precipitation, run_cmip6
from riskclima_spi.config import CMIP6Settings, ERA5Settings
from riskclima_spi.era5 import prepare_era5_monthly_precipitation, run_era5, standardize_era5_dims
from riskclima_spi.pipeline import calculate_spi


def test_cmip6_preparation_converts_flux_and_sums(spi_environment: None) -> None:
    settings = CMIP6Settings()
    time = np.arange("1961-01-01", "1961-02-01", dtype="datetime64[D]").astype("datetime64[ns]")
    dataset = xr.Dataset(
        {"pr": (("time", "lat", "lon"), np.full((len(time), 1, 1), 1e-5))},
        coords={
            "time": time,
            "lat": [0],
            "lon": [0],
        },
    )
    dataset["pr"].attrs["units"] = "kg m-2 s-1"

    monthly = prepare_cmip6_monthly_precipitation(dataset, settings)

    assert monthly.item() == pytest.approx(26.784)
    assert monthly.attrs["units"] == "mm month-1"


def test_cmip6_preparation_rejects_unknown_units(spi_environment: None) -> None:
    settings = CMIP6Settings()
    dataset = xr.Dataset(
        {"pr": (("time", "lat", "lon"), np.ones((1, 1, 1)))},
        coords={
            "time": np.array(["1961-01-01"], dtype="datetime64[ns]"),
            "lat": [0],
            "lon": [0],
        },
    )
    dataset["pr"].attrs["units"] = "mm"

    with pytest.raises(ValueError, match="units"):
        prepare_cmip6_monthly_precipitation(dataset, settings)


def test_cmip6_preparation_rejects_incomplete_month(spi_environment: None) -> None:
    settings = CMIP6Settings()
    dataset = xr.Dataset(
        {"pr": (("time", "lat", "lon"), np.ones((2, 1, 1)))},
        coords={
            "time": np.array(["1961-01-01", "1961-01-02"], dtype="datetime64[ns]"),
            "lat": [0],
            "lon": [0],
        },
    )
    dataset["pr"].attrs["units"] = "kg m-2 s-1"

    with pytest.raises(ValueError, match="every day"):
        prepare_cmip6_monthly_precipitation(dataset, settings)


def test_era5_standardization_renames_and_sorts_dimensions(spi_environment: None) -> None:
    settings = ERA5Settings()
    dataset = _era5_dataset(
        ["2020-02-01", "2020-01-01"],
        latitudes=[20.0, -70.0],
        longitudes=[-5.0, -120.0],
    )

    standardized = standardize_era5_dims(dataset, settings)

    assert tuple(standardized.dims) == ("time", "lat", "lon")
    assert standardized["time"].values.tolist() == sorted(standardized["time"].values.tolist())
    assert standardized["lat"].values.tolist() == [-70.0, 20.0]
    assert standardized["lon"].values.tolist() == [-120.0, -5.0]


def test_era5_standardization_drops_singleton_expver_and_number(spi_environment: None) -> None:
    settings = ERA5Settings()
    dataset = _era5_dataset(["2020-01-01"])
    dataset = dataset.expand_dims(expver=1).assign_coords(number=("expver", [0]))

    standardized = standardize_era5_dims(dataset, settings)

    assert "expver" not in standardized.dims
    assert "expver" not in standardized.coords
    assert "number" not in standardized.coords
    assert tuple(standardized.dims) == ("time", "lat", "lon")


def test_era5_standardization_merges_two_expver_versions(spi_environment: None) -> None:
    settings = ERA5Settings()
    primary = _era5_dataset(["2020-01-01"])
    secondary = _era5_dataset(["2020-01-01"])
    primary["tp"].values[:] = np.nan
    dataset = xr.concat([primary, secondary], dim="expver").assign_coords(
        expver=[1, 5], number=("expver", [0, 1])
    )

    standardized = standardize_era5_dims(dataset, settings)

    assert "expver" not in standardized.dims
    assert "number" not in standardized.coords
    assert float(standardized["tp"].item()) == pytest.approx(0.001)


def test_era5_standardization_rejects_conflicting_expver_values(spi_environment: None) -> None:
    settings = ERA5Settings()
    primary = _era5_dataset(["2020-01-01"])
    secondary = _era5_dataset(["2020-01-01"])
    secondary["tp"].values[:] = 0.002
    dataset = xr.concat([primary, secondary], dim="expver").assign_coords(expver=[1, 5])

    with pytest.raises(ValueError, match="conflicting"):
        standardize_era5_dims(dataset, settings)


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2020-01-01", 31.0),
        ("2020-02-01", 29.0),
        ("2021-02-01", 28.0),
        ("2021-04-01", 30.0),
    ],
)
def test_era5_preparation_converts_mean_daily_to_monthly_accumulation(
    spi_environment: None, timestamp: str, expected: float
) -> None:
    settings = ERA5Settings()
    dataset = _era5_dataset([timestamp])

    monthly = prepare_era5_monthly_precipitation(dataset, settings)

    assert monthly.item() == pytest.approx(expected)
    assert monthly.attrs["units"] == "mm month-1"


def test_calculate_spi_uses_separate_calibration_period(spi_environment: None) -> None:
    settings = CMIP6Settings()
    time = np.arange("1961-01", "2051-01", dtype="datetime64[M]").astype("datetime64[ns]")
    values = np.random.default_rng(7).gamma(2, 20, (len(time), 1, 1))
    precipitation = xr.DataArray(
        values,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": [0], "lon": [0]},
        name="pr",
        attrs={"units": "mm month-1"},
    )

    spi = calculate_spi(precipitation, precipitation, settings)

    assert spi.sizes["time"] == 432
    assert "number_of_zeros" not in spi.coords
    assert "number_of_notnull" not in spi.coords
    assert "prob_of_zero" not in spi.coords


def test_run_era5_writes_spi_from_acquired_monthly_input(
    spi_environment: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERA5_DOWNLOAD_START", "1961-01-01")
    monkeypatch.setenv("ERA5_DOWNLOAD_END", "1991-12-01")
    monkeypatch.setenv("ERA5_CALIBRATION_START", "1961-01-01")
    monkeypatch.setenv("ERA5_CALIBRATION_END", "1990-12-01")
    monkeypatch.setenv("ERA5_APPLICATION_START", "1991-01-01")
    monkeypatch.setenv("ERA5_APPLICATION_END", "1991-12-01")
    settings = ERA5Settings()
    time = np.arange("1961-01", "1992-01", dtype="datetime64[M]").astype("datetime64[ns]")
    values = np.random.default_rng(9).gamma(2, 0.001, (len(time), 1, 1))
    raw_path = tmp_path / "era5.nc"
    dataset = xr.Dataset(
        {"tp": (("time", "lat", "lon"), values)},
        coords={"time": time, "lat": [0.0], "lon": [0.0]},
    )
    dataset["tp"].attrs["units"] = "m"
    dataset.to_netcdf(raw_path)

    def acquired_input(era5_settings: ERA5Settings) -> Path:
        if era5_settings != settings:
            raise ValueError("unexpected settings")
        return raw_path

    monkeypatch.setattr(era5_module, "ensure_era5_input", acquired_input)
    calculate = era5_module.calculate_spi
    write = era5_module.write_output

    def calculate_dask_spi(
        monthly_precipitation: xr.DataArray,
        calibration_precipitation: xr.DataArray,
        calculation_settings: ERA5Settings,
    ) -> xr.DataArray:
        assert isinstance(monthly_precipitation.data, Array)
        assert monthly_precipitation.chunks is not None
        assert monthly_precipitation.chunks[0] == (len(time),)
        return calculate(monthly_precipitation, calibration_precipitation, calculation_settings)

    def write_dask_output(
        output: xr.Dataset,
        output_path: Path,
        output_settings: ERA5Settings,
        *,
        dask_workers: int | None = None,
    ) -> Path:
        assert isinstance(output["spi"].data, Array)
        assert dask_workers == settings.era5_dask_workers
        return write(output, output_path, output_settings, dask_workers=dask_workers)

    monkeypatch.setattr(era5_module, "calculate_spi", calculate_dask_spi)
    monkeypatch.setattr(era5_module, "write_output", write_dask_output)

    output_path = run_era5(settings)

    assert output_path.is_file()
    with xr.open_dataset(output_path) as output:
        assert output.sizes["time"] == 12
        assert output["spi"].attrs["units"] == "1"
        assert output["spi"].attrs["numerical_bounds"] == "[-8.21, 8.21]"
        assert "fit_failure_interpretation" in output["spi"].attrs
        assert "numerical_bounds_interpretation" in output["spi"].attrs
        assert output.attrs["product_type"] == "monthly_averaged_reanalysis"
        assert output.attrs["creator"] == "Test"
        assert output.attrs["code_repository"] == "https://example.org/repo"
        assert output.attrs["input_variables"] == "tp"
        assert output.attrs["input_frequency"] == "monthly"
        assert output.attrs["monthly_precipitation_units"] == "mm month-1"
        assert "effective processing period of one day" in output.attrs["precipitation_conversion"]
        assert "summary" in output.attrs
        assert "creation_date" in output.attrs
        assert "history" in output.attrs
        assert "creators" not in output.attrs
        assert "repository" not in output.attrs


def test_run_cmip6_records_daily_flux_conversion_metadata(
    spi_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = CMIP6Settings()
    calibration_time = np.arange("1961-01-01", "1961-02-01", dtype="datetime64[D]").astype(
        "datetime64[ns]"
    )
    input_time = np.arange("2015-01-01", "2015-02-01", dtype="datetime64[D]").astype(
        "datetime64[ns]"
    )
    _write_cmip6_precipitation(settings.cmip6_calibration_input_file, calibration_time)
    _write_cmip6_precipitation(settings.cmip6_input_file, input_time)

    def calculated_spi(
        monthly_precipitation: xr.DataArray,
        calibration_precipitation: xr.DataArray,
        calculation_settings: CMIP6Settings,
    ) -> xr.DataArray:
        if calculation_settings != settings or calibration_precipitation.sizes["time"] != 1:
            raise ValueError("unexpected CMIP6 calculation inputs")
        return xr.zeros_like(monthly_precipitation).rename("spi")

    monkeypatch.setattr(cmip6_module, "calculate_spi", calculated_spi)

    output_path = run_cmip6(settings)

    with xr.open_dataset(output_path) as output:
        assert output.attrs["dataset_id"] == "CMIP6"
        assert output.attrs["experiment_id"] == "historical"
        assert output.attrs["model_id"] == "ACCESS-CM2"
        assert output.attrs["member_id"] == "ensemble_mean"
        assert output.attrs["grid_label"] == "gn"
        assert output.attrs["input_variables"] == "pr"
        assert output.attrs["input_frequency"] == "daily"
        assert "multiplied by 86400" in output.attrs["precipitation_conversion"]
        assert "resample(time='MS').sum(min_count=1)" in output.attrs["precipitation_conversion"]


def test_run_cmip6_rejects_mismatched_spatial_grids(spi_environment: None) -> None:
    settings = CMIP6Settings()
    calibration_time = np.arange("1961-01-01", "1961-02-01", dtype="datetime64[D]").astype(
        "datetime64[ns]"
    )
    input_time = np.arange("2015-01-01", "2015-02-01", dtype="datetime64[D]").astype(
        "datetime64[ns]"
    )
    _write_cmip6_precipitation(settings.cmip6_calibration_input_file, calibration_time)
    _write_cmip6_precipitation(settings.cmip6_input_file, input_time, latitude=1.0)

    with pytest.raises(ValueError, match="exactly equal latitude and longitude grids"):
        run_cmip6(settings)


def _era5_dataset(
    timestamps: list[str],
    *,
    latitudes: list[float] | None = None,
    longitudes: list[float] | None = None,
) -> xr.Dataset:
    latitudes = latitudes or [0.0]
    longitudes = longitudes or [0.0]
    values = np.full((len(timestamps), len(latitudes), len(longitudes)), 0.001)
    dataset = xr.Dataset(
        {"tp": (("valid_time", "latitude", "longitude"), values)},
        coords={
            "valid_time": np.array(timestamps, dtype="datetime64[ns]"),
            "latitude": latitudes,
            "longitude": longitudes,
        },
    )
    dataset["tp"].attrs["units"] = "m"
    return dataset


def _write_cmip6_precipitation(
    path: Path, time: npt.NDArray[np.datetime64], *, latitude: float = 0.0
) -> None:
    values = np.full((len(time), 1, 1), 1e-5)
    dataset = xr.Dataset(
        {"pr": (("time", "lat", "lon"), values)},
        coords={"time": time, "lat": [latitude], "lon": [0.0]},
    )
    dataset["pr"].attrs["units"] = "kg m-2 s-1"
    dataset.to_netcdf(path)
