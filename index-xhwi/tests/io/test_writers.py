from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.config.settings import CMIP6Settings, ERA5LandSettings, ExistingFilePolicy
from riskclima_xhwi.io.writers import (
    build_monthly_output_dataset,
    concat_monthly_netcdfs,
    normalize_months,
    write_calibration_netcdf,
    write_monthly_netcdf,
)


class TestNormalizeMonths:
    def test_sorts_valid_months(self) -> None:
        assert normalize_months([12, 1, 6]) == [1, 6, 12]

    @pytest.mark.parametrize("months", [[0], [13], [1, 1]])
    def test_rejects_invalid_or_duplicate_months(self, months: list[int]) -> None:
        with pytest.raises(ValueError):
            normalize_months(months)

    def test_builds_part_path_from_configured_template(self) -> None:
        result = ERA5LandSettings().part_output("02")

        assert result == Path("era5land/results/monthly/parts/xhwi_era5land_month_02.nc")


class TestSourceMetadata:
    def test_uses_configured_shared_metadata(self) -> None:
        monthly = xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("time", "lat", "lon"),
            coords={"time": [np.datetime64("2020-01-01")], "lat": [0.0], "lon": [0.0]},
        )
        settings = ERA5LandSettings.model_validate(
            ERA5LandSettings().model_dump()
            | {
                "metadata_project": "Configured project",
                "metadata_license": "Configured license",
                "xhwi_minimum": 0.012,
            }
        )

        result = build_monthly_output_dataset(monthly, settings)

        assert result.attrs["project"] == "Configured project"
        assert result.attrs["license"] == "Configured license"
        assert result.attrs["temperature_threshold_c"] == 32.0
        assert result.attrs["cdf_threshold_percent"] == 95.0
        assert result.attrs["xhwi_minimum"] == settings.xhwi_minimum

    def test_cmip6_metadata_records_identity_interpolation_and_pressure(self) -> None:
        monthly = xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("time", "lat", "lon"),
            coords={"time": [np.datetime64("2050-01-01")], "lat": [0.0], "lon": [0.0]},
        )

        result = build_monthly_output_dataset(
            monthly, CMIP6Settings(_env_file=None, device="cpu"), scenario="ssp585"
        )

        assert result.attrs["model_id"] == "BCC-CSM2-MR"
        assert result.attrs["experiment_id"] == "ssp585"
        assert result.attrs["member_id"] == "r1i1p1f1"
        assert result.attrs["grid_label"] == "gn"
        assert "interpolate('linear')" in result.attrs["temporal_interpolation"]
        assert result.attrs["assumed_pressure_pa"] == 101325.0

    def test_metadata_uses_effective_sources_variables_and_interpolation(self) -> None:
        monthly = xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("time", "lat", "lon"),
            coords={"time": [np.datetime64("2050-01-01")], "lat": [0.0], "lon": [0.0]},
        )
        settings = CMIP6Settings.model_validate(
            CMIP6Settings().model_dump()
            | {
                "variable_tas": "air_temperature",
                "variable_huss": "specific_humidity",
                "variable_tasmax": "maximum_temperature",
                "interpolation_method": "nearest",
                "scenario_tas_template": "input/{model}/{scenario}/{member}/{grid}/tas.zarr",
                "scenario_huss_template": "input/{model}/{scenario}/{member}/{grid}/huss.zarr",
                "calibration_source_template": "calibration/{model}/{grid}/tasmax.zarr",
            }
        )

        result = build_monthly_output_dataset(monthly, settings, scenario="ssp585")

        assert result.attrs["input_variables"] == (
            "air_temperature, specific_humidity, maximum_temperature"
        )
        assert "nearest" in result.attrs["source"]
        assert result.attrs["calibration_source"] == "calibration/BCC-CSM2-MR/gn/tasmax.zarr"

    def test_era5_land_metadata_does_not_claim_era5_store(self) -> None:
        monthly = xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("time", "lat", "lon"),
            coords={"time": [np.datetime64("2020-01-01")], "lat": [0.0], "lon": [0.0]},
        )

        result = build_monthly_output_dataset(
            monthly, ERA5LandSettings(_env_file=None, device="cpu")
        )

        assert result.attrs["dataset_id"] == "ERA5_LAND_ARCO"
        assert "reanalysis_era5_land" in result.attrs["source"]


class TestConcatMonthlyNetcdfs:
    def test_derives_processed_months_from_concatenated_time(self, tmp_path: Path) -> None:
        paths: list[Path] = []
        for month in (3, 1):
            path = tmp_path / f"part-{month}.nc"
            xr.Dataset(
                {
                    "xhwi_monthly_accumulated": (
                        ("time", "lat", "lon"),
                        np.ones((1, 1, 1), dtype=np.float32),
                    )
                },
                coords={
                    "time": [np.datetime64(f"2020-{month:02d}-01")],
                    "lat": [0.0],
                    "lon": [0.0],
                },
            ).to_netcdf(path)
            paths.append(path)

        output = tmp_path / "final.nc"
        concat_monthly_netcdfs(
            paths,
            output,
            settings=ERA5LandSettings(_env_file=None, device="cpu"),
            policy=ExistingFilePolicy.OVERWRITE,
        )

        with xr.open_dataset(output) as result:
            assert result.attrs["processed_calendar_months"] == "01, 03"


class TestNetCDFConfiguration:
    def test_calibration_writer_records_required_identity(self, tmp_path: Path) -> None:
        calibration = xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("calibration_time", "lat", "lon"),
            name="tasmax_calibration",
        )
        settings = CMIP6Settings.model_validate(
            CMIP6Settings().model_dump() | {"netcdf_progress": False}
        )
        output = tmp_path / "calibration.nc"

        write_calibration_netcdf(
            calibration,
            output,
            settings=settings,
            policy=ExistingFilePolicy.OVERWRITE,
        )

        with xr.open_dataarray(output) as result:
            assert result.attrs["calibration_period"] == "1961-01-01 to 1990-12-31"
            assert result.attrs["model_id"] == settings.model
            assert result.attrs["grid_label"] == settings.grid

    @pytest.mark.parametrize(
        ("engine", "format_name", "compression"),
        [
            ("netcdf4", "NETCDF4", True),
            ("netcdf4", "NETCDF4_CLASSIC", False),
            ("scipy", "NETCDF3_CLASSIC", False),
            ("scipy", "NETCDF3_64BIT", False),
        ],
    )
    def test_accepts_executable_combinations(
        self, engine: str, format_name: str, compression: bool
    ) -> None:
        settings = ERA5LandSettings.model_validate(
            ERA5LandSettings().model_dump()
            | {
                "netcdf_engine": engine,
                "netcdf_format": format_name,
                "netcdf_compression": compression,
            }
        )

        assert settings.netcdf_engine == engine

    @pytest.mark.parametrize(
        ("engine", "format_name", "compression"),
        [
            ("netcdf4", "NETCDF3_CLASSIC", False),
            ("scipy", "NETCDF4", False),
            ("scipy", "NETCDF3_CLASSIC", True),
        ],
    )
    def test_rejects_inexecutable_combinations(
        self, engine: str, format_name: str, compression: bool
    ) -> None:
        with pytest.raises(ValueError):
            ERA5LandSettings.model_validate(
                ERA5LandSettings().model_dump()
                | {
                    "netcdf_engine": engine,
                    "netcdf_format": format_name,
                    "netcdf_compression": compression,
                }
            )

    def test_applies_configured_netcdf_dtype(self, tmp_path: Path) -> None:
        dataset = xr.Dataset(
            {
                "xhwi_monthly_accumulated": (
                    ("time", "lat", "lon"),
                    np.ones((1, 1, 1), dtype=np.float32),
                )
            },
            coords={"time": [np.datetime64("2020-01-01")], "lat": [0.0], "lon": [0.0]},
        )
        settings = ERA5LandSettings.model_validate(
            ERA5LandSettings().model_dump()
            | {"netcdf_dtype": "float64", "numpy_dtype": "float64", "netcdf_progress": False}
        )
        output = tmp_path / "configured.nc"

        write_monthly_netcdf(
            dataset,
            output,
            settings=settings,
            policy=ExistingFilePolicy.OVERWRITE,
        )

        with xr.open_dataset(output) as result:
            assert result["xhwi_monthly_accumulated"].dtype == np.dtype("float64")
        assert settings.numpy_type == np.dtype("float64")
