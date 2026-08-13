from importlib import import_module
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from shapely.geometry import box

from riskclima_sacz.libs import era5, gfs, grid, models, ncep, ncep1


@pytest.mark.parametrize("module_name", ["era5", "gfs", "grid", "models", "ncep", "ncep1"])
def test_library_module_imports_from_project_namespace(module_name: str) -> None:
    module = import_module(f"riskclima_sacz.libs.{module_name}")

    assert module.__name__ == f"riskclima_sacz.libs.{module_name}"


def test_era5_catalog_describes_expected_fields() -> None:
    fields = {(collection.variable, collection.level) for collection in era5.feature_collection}

    assert fields == {
        ("div", 200),
        ("div", 850),
        ("u", 200),
        ("u", 850),
        ("v", 200),
        ("v", 850),
        ("vort", 200),
        ("w", 500),
        ("z", 500),
    }
    assert all(collection.features for collection in era5.feature_collection)


def test_feature_collection_validates_catalog_shape() -> None:
    collection = models.FeatureCollection(
        variable="w",
        nc_name="w",
        out_name="omega",
        level=500,
        source="era5",
        features=[models.Feature(area="AB1", shape="AB_OMEGA500_1")],
    )

    assert collection.features[0].shape == "AB_OMEGA500_1"


def test_gridslice_clips_field_and_normalizes_longitude() -> None:
    field = xr.DataArray(
        np.arange(9).reshape(3, 3),
        coords={"latitude": [0.0, 1.0, 2.0], "longitude": [359.0, 360.0, 361.0]},
        dims=("latitude", "longitude"),
    )
    contour = {"geometry": box(359.5, 0.5, 360.5, 1.5)}

    sliced = grid.gridslice(field, contour)

    assert sliced.sizes == {"latitude": 1, "longitude": 1}
    assert sliced.longitude.item() == pytest.approx(0.0)


@pytest.mark.parametrize("downloader", [ncep.download, ncep1.download])
def test_ncep_download_writes_response_and_creates_directory(
    downloader, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class Response:
        content = b"netcdf-content"

        def raise_for_status(self) -> None:
            return None

    requested_urls: list[str] = []

    def get(url: str, *, timeout: int) -> Response:
        requested_urls.append(url)
        assert timeout == 120
        return Response()

    monkeypatch.setattr("requests.get", get)
    output_path = tmp_path / "nested"

    downloader(2020, 2020, ["omega"], output_path)

    assert (output_path / "omega2020.nc").read_bytes() == b"netcdf-content"
    assert len(requested_urls) == 1


def test_gfs_download_rejects_nonpositive_timestep(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timestep must be greater than zero"):
        gfs.rda_download(
            dates=[],
            timestep=0,
            output_path=tmp_path,
            login="user@example.com",
            password="secret",
        )
