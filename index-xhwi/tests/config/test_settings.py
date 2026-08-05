from pathlib import Path

import pytest
import torch
from dotenv import dotenv_values
from pydantic import ValidationError

from riskclima_xhwi.config.settings import (
    CDSCredentials,
    CMIP6Settings,
    Device,
    ERA5LandSettings,
    ERA5Settings,
)

ENV_EXAMPLE = Path(__file__).parents[2] / ".env.example"


class TestSettingsContract:
    def test_env_example_exactly_covers_all_settings(self) -> None:
        example_keys = set(dotenv_values(ENV_EXAMPLE))
        expected: set[str] = set()
        for settings_type, prefix in (
            (ERA5Settings, "ERA5_"),
            (ERA5LandSettings, "ERA5LAND_"),
            (CMIP6Settings, "CMIP6_"),
        ):
            for name, field in settings_type.model_fields.items():
                alias = field.validation_alias
                expected.add(alias if isinstance(alias, str) else f"{prefix}{name.upper()}")
        expected.update({"CDSAPI_KEY", "CDSAPI_CONFIG_FILE"})

        assert example_keys == expected

    def test_new_instances_reread_dotenv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ERA5_DEVICE")
        env_file = tmp_path / ".env"
        env_file.write_text("ERA5_DEVICE=cpu\n", encoding="utf-8")
        first = ERA5Settings(_env_file=env_file)
        env_file.write_text("ERA5_DEVICE=auto\n", encoding="utf-8")

        second = ERA5Settings(_env_file=env_file)

        assert first.device is Device.CPU
        assert second.device is Device.AUTO

    def test_rejects_unknown_template_placeholder(self) -> None:
        settings = ERA5Settings()

        with pytest.raises(ValidationError, match="Unknown path template placeholders"):
            ERA5Settings.model_validate(
                settings.model_dump() | {"part_file_template": "parts/{unsupported}/part.nc"}
            )

    def test_calibration_filename_tracks_effective_years(self) -> None:
        settings = ERA5Settings.model_validate(
            ERA5Settings().model_dump()
            | {"calibration_start": "1981-01-01", "calibration_end": "2010-12-31"}
        )

        assert settings.calibration_output.name == "xhwi_era5_calib_t2m_max_1981-2010.nc"

    def test_empty_application_bounds_mean_full_period(self) -> None:
        assert ERA5Settings().application_period == (None, None)

    def test_example_preserves_current_layout(self) -> None:
        era5 = ERA5Settings()
        land = ERA5LandSettings()
        cmip6 = CMIP6Settings()

        assert era5.calibration_output == Path("era5/raw_data/xhwi_era5_calib_t2m_max_1961-1990.nc")
        assert land.part_output(3) == Path(
            "era5land/results/monthly/parts/xhwi_era5land_month_03.nc"
        )
        assert cmip6.calibration_source == Path(
            "cmip6/BCC-CSM2-MR/historical/day/tasmax/gn/ensemble_mean.zarr"
        )
        assert cmip6.monthly_output("ssp585") == Path(
            "cmip6/BCC-CSM2-MR/results/xhwi_torch/"
            "xhwi_cmip6_BCC-CSM2-MR_ssp585_r1i1p1f1_monthly_accumulated_torch.nc"
        )

    def test_auto_device_uses_available_backend(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(torch.cuda, "is_available", lambda: False)

        assert ERA5Settings().resolve_device() == torch.device("cpu")

    @pytest.mark.parametrize("field", ["source_id", "model", "member", "grid"])
    def test_rejects_unsafe_identifiers(self, field: str) -> None:
        with pytest.raises(ValidationError, match="safe path segment"):
            CMIP6Settings.model_validate(CMIP6Settings().model_dump() | {field: "../unsafe"})

    def test_rejects_duplicate_scenarios(self) -> None:
        with pytest.raises(ValidationError, match="unique"):
            CMIP6Settings.model_validate(
                CMIP6Settings().model_dump() | {"scenarios": ["historical", "historical"]}
            )

    def test_xhwi_minimum_is_loaded_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("XHWI_MINIMUM", "0.01")

        settings = ERA5Settings()

        assert settings.xhwi_minimum == 0.01


class TestCDSCredentials:
    def test_reads_cdsapi_key_from_dotenv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CDSAPI_KEY")
        env_file = tmp_path / ".env"
        env_file.write_text("CDSAPI_KEY=local-secret\n", encoding="utf-8")

        credentials = CDSCredentials(_env_file=env_file)

        assert credentials.cdsapi_key is not None
        assert credentials.cdsapi_key.get_secret_value() == "local-secret"
        assert "local-secret" not in repr(credentials)
