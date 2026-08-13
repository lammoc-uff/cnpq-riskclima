from pathlib import Path

from riskclima_sacz.config import SACZSettings, parse_settings


def test_settings_load_defaults() -> None:
    settings = SACZSettings()

    assert settings.era5_year is None
    assert settings.cmip6_source_id == "BCC-CSM2-MR"
    assert settings.cmip6_experiment_id == "ssp245"


def test_cli_arguments_override_settings(monkeypatch) -> None:
    monkeypatch.setenv("ERA5_YEAR", "2020")

    settings = parse_settings(["--era5-year", "2021", "--cmip6-source-id", "TEST"])

    assert settings.era5_year == 2021
    assert settings.cmip6_source_id == "TEST"


def test_relative_paths_resolve_from_base_directory() -> None:
    settings = SACZSettings(base_directory=Path("/tmp/sacz"))

    assert settings.path(settings.cmip6_input_directory) == Path("/tmp/sacz/cmip6/processed_data")
