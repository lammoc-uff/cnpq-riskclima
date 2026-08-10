from pathlib import Path

import pytest

from riskclima_blocking.config import BlockingSettings


def test_blocking_defaults_use_confirmed_climatology_periods() -> None:
    settings = BlockingSettings()

    assert (settings.era5_climatology_start_year, settings.era5_climatology_end_year) == (
        1991,
        2020,
    )
    assert settings.era5_climatology_label == "90_20"
    assert (settings.cmip6_climatology_start_year, settings.cmip6_climatology_end_year) == (
        1980,
        2010,
    )


def test_environment_values_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMIP6_EXPERIMENT_ID", "ssp245")
    monkeypatch.setenv("ERA5_CLIMATOLOGY_LABEL", "old")

    settings = BlockingSettings()

    assert settings.cmip6_experiment_id == "ssp245"
    assert settings.era5_climatology_label == "old"


def test_relative_paths_resolve_from_base_directory() -> None:
    settings = BlockingSettings(base_directory=Path("/tmp/blocking"))

    assert settings.path(settings.era5_results_directory) == Path("/tmp/blocking/era5/results")
