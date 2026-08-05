from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.config.settings import (
    CalibrationPolicy,
    CMIP6Settings,
    ERA5Settings,
    ExistingFilePolicy,
)
from riskclima_xhwi.io.writers import prepare_output_path
from riskclima_xhwi.pipeline.policies import resolve_calibration, validate_calibration_file


def settings_for_policy(tmp_path: Path, policy: CalibrationPolicy) -> ERA5Settings:
    return ERA5Settings.model_validate(
        ERA5Settings().model_dump()
        | {
            "calibration_policy": policy,
            "calibration_file_template": str(tmp_path / "calibration-{start_year}-{end_year}.nc"),
        }
    )


def write_valid_calibration(path: Path) -> Path:
    xr.DataArray(
        np.ones((1, 1, 1), dtype=np.float32),
        dims=("calibration_time", "lat", "lon"),
        coords={"calibration_time": [np.datetime64("1961-01-01")], "lat": [0.0], "lon": [0.0]},
        attrs={"calibration_period": "1961-01-01 to 1990-12-31"},
    ).to_netcdf(path)
    return path


class TestCalibrationPolicy:
    def test_require_existing_accepts_valid_file(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.REQUIRE_EXISTING)
        write_valid_calibration(settings.calibration_output)

        assert (
            resolve_calibration(settings, lambda: settings.calibration_output)
            == settings.calibration_output
        )

    def test_require_existing_fails_for_missing_file(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.REQUIRE_EXISTING)

        with pytest.raises(FileNotFoundError):
            resolve_calibration(settings, lambda: settings.calibration_output)

    def test_create_if_missing_creates_once(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.CREATE_IF_MISSING)
        calls = 0

        def create() -> Path:
            nonlocal calls
            calls += 1
            return write_valid_calibration(settings.calibration_output)

        resolve_calibration(settings, create)
        resolve_calibration(settings, create)

        assert calls == 1

    def test_rebuild_always_creates(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.REBUILD)
        calls = 0

        def create() -> Path:
            nonlocal calls
            calls += 1
            return write_valid_calibration(settings.calibration_output)

        resolve_calibration(settings, create)
        resolve_calibration(settings, create)

        assert calls == 2

    def test_in_memory_neither_requires_nor_creates_file(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.IN_MEMORY)

        assert resolve_calibration(settings, lambda: settings.calibration_output) is None

    def test_rejects_missing_calibration_period(self, tmp_path: Path) -> None:
        settings = settings_for_policy(tmp_path, CalibrationPolicy.REQUIRE_EXISTING)
        xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("calibration_time", "lat", "lon"),
        ).to_netcdf(settings.calibration_output)

        with pytest.raises(ValueError, match="calibration_period"):
            validate_calibration_file(settings.calibration_output, settings)

    @pytest.mark.parametrize("missing_attribute", ["grid_label", "model_id"])
    def test_cmip6_requires_identity_metadata(self, tmp_path: Path, missing_attribute: str) -> None:
        settings = CMIP6Settings.model_validate(
            CMIP6Settings().model_dump()
            | {"calibration_file_template": str(tmp_path / "{model}-{start_year}-{end_year}.nc")}
        )
        attrs = {
            "calibration_period": "1961-01-01 to 1990-12-31",
            "grid_label": settings.grid,
            "model_id": settings.model,
        }
        del attrs[missing_attribute]
        xr.DataArray(
            np.ones((1, 1, 1)),
            dims=("calibration_time", "lat", "lon"),
            attrs=attrs,
        ).to_netcdf(settings.calibration_output)

        with pytest.raises(ValueError, match=missing_attribute):
            validate_calibration_file(settings.calibration_output, settings)


class TestExistingFilePolicy:
    @pytest.mark.parametrize(
        ("policy", "should_write"),
        [(ExistingFilePolicy.SKIP, False), (ExistingFilePolicy.OVERWRITE, True)],
    )
    def test_handles_existing_output(
        self, tmp_path: Path, policy: ExistingFilePolicy, should_write: bool
    ) -> None:
        path = tmp_path / "output.nc"
        path.touch()

        _, result = prepare_output_path(path, policy)

        assert result is should_write
        assert path.exists() is (policy is ExistingFilePolicy.SKIP)

    def test_fail_rejects_existing_output(self, tmp_path: Path) -> None:
        path = tmp_path / "output.nc"
        path.touch()

        with pytest.raises(FileExistsError):
            prepare_output_path(path, ExistingFilePolicy.FAIL)
