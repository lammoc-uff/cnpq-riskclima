from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def spi_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Configure complete CMIP6 and ERA5 test environments."""
    values = {
        "SPI_SCALE_MONTHS": "1",
        "SPI_DISTRIBUTION": "gamma",
        "SPI_METHOD": "APP",
        "SPI_FLOC": "0",
        "NETCDF_ENGINE": "netcdf4",
        "NETCDF_FORMAT": "NETCDF4",
        "NETCDF_COMPRESSION": "true",
        "NETCDF_COMPLEVEL": "4",
        "METADATA_CREATORS": "Test",
        "METADATA_INSTITUTION": "Test institution",
        "METADATA_PROJECT": "Test project",
        "METADATA_LICENSE": "CC-BY-4.0",
        "METADATA_REFERENCES": "https://example.org",
        "METADATA_REPOSITORY": "https://example.org/repo",
        "METADATA_CONVENTIONS": "CF-1.10",
        "METADATA_PROCESSING_LEVEL": "Processed data",
        "LOG_LEVEL": "INFO",
        "LOG_FORMAT": "%(levelname)s: %(message)s",
        "CMIP6_INPUT_FILE": str(tmp_path / "input.nc"),
        "CMIP6_CALIBRATION_INPUT_FILE": str(tmp_path / "calibration.nc"),
        "CMIP6_MODEL": "ACCESS-CM2",
        "CMIP6_EXPERIMENT": "historical",
        "CMIP6_MEMBER": "ensemble_mean",
        "CMIP6_GRID": "gn",
        "CMIP6_PRECIPITATION_VARIABLE": "pr",
        "CMIP6_TIME_DIMENSION": "time",
        "CMIP6_LATITUDE_DIMENSION": "lat",
        "CMIP6_LONGITUDE_DIMENSION": "lon",
        "CMIP6_CALIBRATION_START": "1961-01-01",
        "CMIP6_CALIBRATION_END": "1990-12-31",
        "CMIP6_APPLICATION_START": "2015-01-01",
        "CMIP6_APPLICATION_END": "2050-12-31",
        "CMIP6_OUTPUT_DIRECTORY": str(tmp_path / "cmip6-results"),
        "CMIP6_OUTPUT_TEMPLATE": (
            "spi{scale_months}_{model}_{experiment}_{member}_{grid}_{start}_{end}.nc"
        ),
        "ERA5_DATASET": "reanalysis-era5-single-levels-monthly-means",
        "ERA5_PRODUCT_TYPE": "monthly_averaged_reanalysis",
        "ERA5_REQUEST_VARIABLE": "total_precipitation",
        "ERA5_DOWNLOAD_START": "2020-01-01",
        "ERA5_DOWNLOAD_END": "2021-02-01",
        "ERA5_TIME": "00:00",
        "ERA5_DATA_FORMAT": "netcdf",
        "ERA5_DOWNLOAD_FORMAT": "unarchived",
        "ERA5_LATITUDE_MIN": "-70",
        "ERA5_LATITUDE_MAX": "20",
        "ERA5_LONGITUDE_MIN": "-120",
        "ERA5_LONGITUDE_MAX": "-5",
        "ERA5_RAW_FILE_TEMPLATE": str(tmp_path / "era5_tp_{start}_{end}.nc"),
        "ERA5_SPATIAL_CHUNK": "32",
        "ERA5_DASK_WORKERS": "2",
        "ERA5_PRECIPITATION_VARIABLE": "tp",
        "ERA5_TIME_DIMENSION": "valid_time",
        "ERA5_LATITUDE_DIMENSION": "latitude",
        "ERA5_LONGITUDE_DIMENSION": "longitude",
        "ERA5_CALIBRATION_START": "2020-01-01",
        "ERA5_CALIBRATION_END": "2020-12-01",
        "ERA5_APPLICATION_START": "2021-01-01",
        "ERA5_APPLICATION_END": "2021-02-01",
        "ERA5_OUTPUT_DIRECTORY": str(tmp_path / "era5-results"),
        "ERA5_OUTPUT_TEMPLATE": "spi{scale_months}_era5_{start}_{end}.nc",
        "CDSAPI_URL": "https://example.org/api",
        "CDSAPI_KEY": "test-key",
        "CDSAPI_CONFIG_FILE": str(tmp_path / ".cdsapirc"),
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    yield
