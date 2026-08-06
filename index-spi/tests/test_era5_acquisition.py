import logging
from pathlib import Path
from typing import ClassVar, override

import numpy as np
import pytest
import xarray as xr

import riskclima_spi.era5 as era5_module
from riskclima_spi.config import CDSCredentials, ERA5Settings
from riskclima_spi.era5 import (
    CDSClientAdapter,
    CDSRequest,
    _create_cds_client,
    ensure_era5_input_with_client,
)


class FakeRawCDSClient:
    """Record direct retrieval arguments without returning a CDS result."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, CDSRequest, str]] = []

    def retrieve(self, dataset: str, request: CDSRequest, target: str) -> object:
        self.calls.append((dataset, request, target))
        Path(target).write_text("downloaded", encoding="utf-8")
        return 42


class FakeCDSClient:
    """Record requests while producing local test downloads."""

    def __init__(self) -> None:
        self.requests: list[CDSRequest] = []

    def download(self, dataset: str, request: CDSRequest, target: str) -> None:
        if dataset != "reanalysis-era5-single-levels-monthly-means":
            raise ValueError("unexpected dataset")
        self.requests.append(request)
        years = _string_values(request, "year")
        months = _string_values(request, "month")
        timestamps = [f"{year}-{month}-01" for year in years for month in months]
        values = np.full((len(timestamps), 2, 2), 0.001)
        downloaded_dataset = xr.Dataset(
            {"tp": (("valid_time", "latitude", "longitude"), values)},
            coords={
                "valid_time": np.array(timestamps, dtype="datetime64[ns]"),
                "latitude": [20.0, -70.0],
                "longitude": [-5.0, -120.0],
            },
        )
        downloaded_dataset["tp"].attrs["units"] = "m"
        downloaded_dataset.to_netcdf(target)


class FailingCDSClient(FakeCDSClient):
    """Fail the second request after creating the first temporary part."""

    @override
    def download(self, dataset: str, request: CDSRequest, target: str) -> None:
        if self.requests:
            raise RuntimeError("CDS unavailable")
        super().download(dataset, request, target)


class MismatchedGridCDSClient(FakeCDSClient):
    """Produce a shifted grid for the current-year part."""

    @override
    def download(self, dataset: str, request: CDSRequest, target: str) -> None:
        super().download(dataset, request, target)
        if len(self.requests) == 2:
            with xr.open_dataset(target) as downloaded:
                shifted = downloaded.load().assign_coords(latitude=[21.0, -70.0])
            shifted.to_netcdf(target)


class RecordingRawCDSClient:
    """Record endpoint and key passed to the CDS constructor."""

    calls: ClassVar[list[tuple[str, str]]] = []

    def __init__(self, *, url: str, key: str) -> None:
        self.calls.append((url, key))

    def retrieve(self, dataset: str, request: CDSRequest, target: str) -> object:
        raise RuntimeError("retrieval is not expected in credential tests")


def test_cds_client_adapter_retrieves_directly_to_target(tmp_path: Path) -> None:
    raw_client = FakeRawCDSClient()
    adapter = CDSClientAdapter(raw_client)
    request: CDSRequest = {"year": ["2020"]}
    target = tmp_path / "download.nc"

    adapter.download("dataset", request, str(target))

    assert raw_client.calls == [("dataset", request, str(target))]
    assert target.read_text(encoding="utf-8") == "downloaded"


def test_era5_acquisition_downloads_two_parts_and_removes_them(
    spi_environment: None, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = ERA5Settings()
    client = FakeCDSClient()

    with caplog.at_level(logging.INFO):
        output = ensure_era5_input_with_client(settings, client)

    assert len(client.requests) == 2
    assert _string_values(client.requests[0], "year") == ["2020"]
    assert _string_values(client.requests[0], "month") == [f"{month:02d}" for month in range(1, 13)]
    assert _string_values(client.requests[1], "year") == ["2021"]
    assert _string_values(client.requests[1], "month") == ["01", "02"]
    assert output.is_file()
    assert list(tmp_path.glob("*.part.nc")) == []
    assert "ERA5 input file created successfully" in caplog.text
    with xr.open_dataset(output) as dataset:
        assert tuple(dataset.dims) == ("time", "lat", "lon")
        assert dataset.sizes["time"] == 14
        assert dataset["lat"].values.tolist() == [-70.0, 20.0]
        assert dataset.attrs["era5_dataset"] == "reanalysis-era5-single-levels-monthly-means"


def test_repeated_era5_acquisition_downloads_again_and_overwrites_final(
    spi_environment: None,
) -> None:
    settings = ERA5Settings()
    first_client = FakeCDSClient()
    output = ensure_era5_input_with_client(settings, first_client)
    second_client = FakeCDSClient()

    overwritten = ensure_era5_input_with_client(settings, second_client)

    assert overwritten == output
    assert len(first_client.requests) == 2
    assert len(second_client.requests) == 2
    assert list(output.parent.glob("*.part.nc")) == []


def test_different_era5_period_creates_new_final_file(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_settings = ERA5Settings()
    first_output = ensure_era5_input_with_client(first_settings, FakeCDSClient())
    monkeypatch.setenv("ERA5_DOWNLOAD_END", "2021-03-01")
    monkeypatch.setenv("ERA5_APPLICATION_END", "2021-03-01")
    second_settings = ERA5Settings()

    second_output = ensure_era5_input_with_client(second_settings, FakeCDSClient())

    assert first_output != second_output
    assert first_output.is_file()
    assert second_output.is_file()


def test_different_request_with_same_period_overwrites_exact_final(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_settings = ERA5Settings()
    first_output = ensure_era5_input_with_client(first_settings, FakeCDSClient())
    monkeypatch.setenv("ERA5_LONGITUDE_MAX", "-10")
    second_settings = ERA5Settings()

    second_output = ensure_era5_input_with_client(second_settings, FakeCDSClient())

    assert first_output == second_output
    assert second_output.is_file()
    with xr.open_dataset(second_output) as dataset:
        assert dataset.attrs["era5_area"].endswith("-10.0")


def test_failed_download_removes_parts_without_creating_final(
    spi_environment: None, tmp_path: Path
) -> None:
    settings = ERA5Settings()

    with pytest.raises(RuntimeError, match="CDS unavailable"):
        ensure_era5_input_with_client(settings, FailingCDSClient())

    assert not settings.raw_input_path().exists()
    assert list(tmp_path.glob("*.part.nc")) == []


def test_failed_download_preserves_existing_final_until_atomic_replace(
    spi_environment: None, tmp_path: Path
) -> None:
    settings = ERA5Settings()
    output = ensure_era5_input_with_client(settings, FakeCDSClient())
    original = output.read_bytes()

    with pytest.raises(RuntimeError, match="CDS unavailable"):
        ensure_era5_input_with_client(settings, FailingCDSClient())

    assert output.read_bytes() == original
    assert list(tmp_path.glob("*.part.nc")) == []
    assert list(tmp_path.glob("*.tmp")) == []


def test_incompatible_era5_part_grids_fail_and_cleanup(
    spi_environment: None, tmp_path: Path
) -> None:
    settings = ERA5Settings()

    with pytest.raises(ValueError, match="exactly equal latitude and longitude grids"):
        ensure_era5_input_with_client(settings, MismatchedGridCDSClient())

    assert not settings.raw_input_path().exists()
    assert list(tmp_path.glob("*.part.nc")) == []
    assert list(tmp_path.glob("*.tmp")) == []


def test_cds_environment_key_has_priority_over_missing_config_file(
    spi_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    RecordingRawCDSClient.calls.clear()
    monkeypatch.setattr(era5_module.cdsapi, "Client", RecordingRawCDSClient)
    monkeypatch.setenv("CDSAPI_CONFIG_FILE", "/does/not/exist")

    _create_cds_client(CDSCredentials())

    assert RecordingRawCDSClient.calls == [("https://example.org/api", "test-key")]


def test_cds_config_file_supplies_only_fallback_key(
    spi_environment: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    RecordingRawCDSClient.calls.clear()
    monkeypatch.setattr(era5_module.cdsapi, "Client", RecordingRawCDSClient)
    config_path = tmp_path / ".cdsapirc"
    config_path.write_text("url: https://ignored.example/api\nkey: file-key\n", encoding="utf-8")
    monkeypatch.setenv("CDSAPI_KEY", "")

    _create_cds_client(CDSCredentials())

    assert RecordingRawCDSClient.calls == [("https://example.org/api", "file-key")]


def _string_values(request: CDSRequest, key: str) -> list[str]:
    values = request[key]
    if not isinstance(values, list):
        raise TypeError(f"request field {key!r} is not a string list")
    strings: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"request field {key!r} is not a string list")
        strings.append(value)
    return strings
