from pathlib import Path

import pytest

from riskclima_spi.config import CMIP6Settings, ERA5Settings


def test_cmip6_output_path_contains_experiment_and_period(spi_environment: None) -> None:
    settings = CMIP6Settings()

    assert settings.output_path().name == (
        "spi1_ACCESS-CM2_historical_ensemble_mean_gn_2015-01-01_2050-12-31.nc"
    )


def test_era5_paths_contain_complete_period(spi_environment: None) -> None:
    settings = ERA5Settings()

    assert settings.raw_input_path().name == "era5_tp_2020-01-01_2021-02-01.nc"
    assert settings.output_path().name == "spi1_era5_2021-01-01_2021-02-01.nc"
    assert settings.era5_spatial_chunk == 32
    assert settings.era5_dask_workers == 2


def test_era5_dask_settings_load_from_environment(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ERA5_SPATIAL_CHUNK", "64")
    monkeypatch.setenv("ERA5_DASK_WORKERS", "3")

    settings = ERA5Settings()

    assert settings.era5_spatial_chunk == 64
    assert settings.era5_dask_workers == 3


def test_shared_shapefile_settings_apply_to_both_sources(
    spi_environment: None,
    boundary_shapefile: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPI_USE_SHAPEFILE", "true")
    monkeypatch.setenv("SPI_SHAPEFILE_PATH", str(boundary_shapefile))

    cmip6 = CMIP6Settings()
    era5 = ERA5Settings()

    assert cmip6.spi_use_shapefile is True
    assert era5.spi_use_shapefile is True
    assert cmip6.spi_shapefile_path == boundary_shapefile
    assert era5.spi_shapefile_path == boundary_shapefile


def test_disabled_shapefile_does_not_require_the_file(spi_environment: None) -> None:
    settings = CMIP6Settings()

    assert settings.spi_use_shapefile is False
    assert not settings.spi_shapefile_path.exists()


def test_enabled_shapefile_rejects_missing_file(
    spi_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPI_USE_SHAPEFILE", "true")

    with pytest.raises(ValueError, match="SPI shapefile does not exist"):
        ERA5Settings()


@pytest.mark.parametrize("name", ["CMIP6_MODEL", "CMIP6_MEMBER", "CMIP6_GRID"])
def test_cmip6_settings_reject_blank_identity(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv(name, "   ")

    with pytest.raises(ValueError, match="must not be blank"):
        CMIP6Settings()


def test_cmip6_settings_reject_output_template_missing_identity_placeholder(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "CMIP6_OUTPUT_TEMPLATE",
        "spi{scale_months}_{model}_{experiment}_{member}_{start}_{end}.nc",
    )

    with pytest.raises(ValueError, match="missing required placeholders: grid"):
        CMIP6Settings()


def test_cmip6_settings_reject_output_template_unknown_placeholder(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "CMIP6_OUTPUT_TEMPLATE",
        "spi{scale_months}_{model}_{experiment}_{member}_{grid}_{start}_{end}_{variable}.nc",
    )

    with pytest.raises(ValueError, match="unknown placeholders: variable"):
        CMIP6Settings()


def test_settings_reject_reversed_application_period(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CMIP6_APPLICATION_START", "2051-01-01")

    with pytest.raises(ValueError, match="application start"):
        CMIP6Settings()


def test_era5_settings_reject_partial_start_month(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ERA5_DOWNLOAD_START", "2020-02-01")

    with pytest.raises(ValueError, match="January 1"):
        ERA5Settings()


def test_env_example_contains_every_settings_alias(spi_environment: None) -> None:
    env_text = (Path(__file__).parents[1] / ".env.example").read_text(encoding="utf-8")

    for name in (
        "CMIP6_INPUT_FILE",
        "CMIP6_CALIBRATION_INPUT_FILE",
        "CMIP6_MODEL",
        "CMIP6_MEMBER",
        "CMIP6_GRID",
        "ERA5_DATASET",
        "ERA5_DOWNLOAD_START",
        "ERA5_DOWNLOAD_END",
        "ERA5_RAW_FILE_TEMPLATE",
        "ERA5_SPATIAL_CHUNK",
        "ERA5_DASK_WORKERS",
        "ERA5_OUTPUT_TEMPLATE",
        "SPI_USE_SHAPEFILE",
        "SPI_SHAPEFILE_PATH",
    ):
        assert f"{name}=" in env_text
    assert 'METADATA_CREATORS="Marcio Cataldi <mcataldi@id.uff.br>"' in env_text
    assert "ERA5_SPATIAL_CHUNK=32" in env_text
    assert "ERA5_DASK_WORKERS=1" in env_text
    assert "SPI_USE_SHAPEFILE=True" in env_text
    assert "SPI_SHAPEFILE_PATH=geo_data/brazil/BR_Pais_2025.shp" in env_text
