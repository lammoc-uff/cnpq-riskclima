from pathlib import Path

import dask.array as da
import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from shapely.geometry import Polygon

from riskclima_spi.spatial import (
    GeographicBounds,
    resolve_spatial_selection,
    select_spatial_domain,
)


def test_bundled_brazil_boundary_is_complete_and_readable() -> None:
    path = Path(__file__).parents[1] / "geo_data" / "brazil" / "BR_Pais_2025.shp"

    for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        assert path.with_suffix(suffix).is_file()

    selection = resolve_spatial_selection(
        use_shapefile=True,
        shapefile_path=path,
        fallback_bounds=GeographicBounds(west=-75, south=-35, east=-33, north=7),
    )

    assert selection.geometry_crs == "EPSG:4674"
    assert selection.bounds.west == pytest.approx(-73.9868097)
    assert selection.bounds.south == pytest.approx(-33.751178)
    assert selection.bounds.east == pytest.approx(-28.8476399)
    assert selection.bounds.north == pytest.approx(5.2696195)


def test_shapefile_selection_uses_geometry_bounds_and_cell_centers(
    boundary_shapefile: Path,
) -> None:
    selection = resolve_spatial_selection(
        use_shapefile=True,
        shapefile_path=boundary_shapefile,
        fallback_bounds=GeographicBounds(west=-10, south=-10, east=10, north=10),
    )
    values = da.ones((1, 3, 3), chunks=(1, 3, 3))
    data = xr.DataArray(
        values,
        dims=("time", "lat", "lon"),
        coords={"time": [0], "lat": [-1.0, 0.0, 1.0], "lon": [-1.0, 0.0, 1.0]},
    )

    selected = select_spatial_domain(data, selection)

    assert selection.bounds == GeographicBounds(west=-1, south=-1, east=1, north=1)
    assert selection.geometry_crs == "EPSG:4674"
    assert selected.chunks is not None
    computed = selected.compute()
    assert computed.sel(lat=0, lon=0).item() == 1
    assert computed.sel(lat=1, lon=-1).item() == 1
    assert np.isnan(computed.sel(lat=1, lon=1).item())


def test_disabled_shapefile_uses_fallback_bounds() -> None:
    fallback = GeographicBounds(west=-1, south=-1, east=1, north=1)
    selection = resolve_spatial_selection(
        use_shapefile=False,
        shapefile_path=Path("missing.shp"),
        fallback_bounds=fallback,
    )
    data = xr.DataArray(
        np.ones((3, 3)),
        dims=("lat", "lon"),
        coords={"lat": [-2, 0, 2], "lon": [-2, 0, 2]},
    )

    selected = select_spatial_domain(data, selection)

    assert selection.bounds == fallback
    assert selection.uses_shapefile is False
    assert selected.sizes == {"lat": 1, "lon": 1}


def test_shapefile_without_crs_fails(tmp_path: Path) -> None:
    path = tmp_path / "missing-crs.shp"
    frame = gpd.GeoDataFrame(geometry=[Polygon([(-1, -1), (1, -1), (-1, 1), (-1, -1)])])
    frame.to_file(path)

    with pytest.raises(ValueError, match="coordinate reference system"):
        resolve_spatial_selection(
            use_shapefile=True,
            shapefile_path=path,
            fallback_bounds=GeographicBounds(west=-10, south=-10, east=10, north=10),
        )


def test_shapefile_selection_rejects_domain_without_cell_centers(
    boundary_shapefile: Path,
) -> None:
    selection = resolve_spatial_selection(
        use_shapefile=True,
        shapefile_path=boundary_shapefile,
        fallback_bounds=GeographicBounds(west=-10, south=-10, east=10, north=10),
    )
    data = xr.DataArray(
        np.ones((1, 1)),
        dims=("lat", "lon"),
        coords={"lat": [0.75], "lon": [0.75]},
    )

    with pytest.raises(ValueError, match="does not contain any grid-cell centers"):
        select_spatial_domain(data, selection)
