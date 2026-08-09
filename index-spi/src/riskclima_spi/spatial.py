from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import xarray as xr
from shapely import covers, points
from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class GeographicBounds:
    """Geographic extent in decimal degrees."""

    west: float
    south: float
    east: float
    north: float

    def as_cds_area(self) -> list[float]:
        """Return bounds in the north, west, south, east order required by CDS."""
        return [self.north, self.west, self.south, self.east]


@dataclass(frozen=True)
class SpatialSelection:
    """Resolved rectangular extent and optional territorial geometry."""

    bounds: GeographicBounds
    geometry: BaseGeometry | None = None
    geometry_path: Path | None = None
    geometry_crs: str | None = None

    @property
    def uses_shapefile(self) -> bool:
        """Return whether the selection includes a territorial mask."""
        return self.geometry is not None


def resolve_spatial_selection(
    *,
    use_shapefile: bool,
    shapefile_path: Path,
    fallback_bounds: GeographicBounds,
) -> SpatialSelection:
    """Resolve either configured bounds or a validated shapefile boundary.

    Parameters
    ----------
    use_shapefile
        Use the shapefile geometry and its bounds when true.
    shapefile_path
        Path to the main ``.shp`` file.
    fallback_bounds
        Source-specific bounds used when shapefile clipping is disabled.

    Returns
    -------
    SpatialSelection
        Bounds and optional geometry shared by acquisition and preprocessing.
    """
    if not use_shapefile:
        return SpatialSelection(bounds=fallback_bounds)

    frame = gpd.read_file(shapefile_path.expanduser())
    if frame.empty:
        raise ValueError("SPI shapefile must contain at least one feature")
    if frame.crs is None:
        raise ValueError("SPI shapefile must declare a coordinate reference system")
    if bool(frame.geometry.isna().any()):
        raise ValueError("SPI shapefile must not contain null geometries")

    source_crs = str(frame.crs)
    geometry = frame.to_crs("EPSG:4326").geometry.union_all()
    if geometry.is_empty:
        raise ValueError("SPI shapefile geometry must not be empty")
    if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError("SPI shapefile must contain polygon geometry")
    if not geometry.is_valid:
        raise ValueError("SPI shapefile geometry must be valid")

    west, south, east, north = (float(value) for value in geometry.bounds)
    values = np.array([west, south, east, north])
    if not np.isfinite(values).all():
        raise ValueError("SPI shapefile bounds must be finite")
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("SPI shapefile bounds must use geographic longitude and latitude")

    return SpatialSelection(
        bounds=GeographicBounds(west=west, south=south, east=east, north=north),
        geometry=geometry,
        geometry_path=shapefile_path,
        geometry_crs=source_crs,
    )


def select_spatial_domain(
    data: xr.DataArray,
    selection: SpatialSelection,
) -> xr.DataArray:
    """Subset a rectilinear grid and apply its optional territorial mask.

    A cell is retained when its center is inside or on the boundary of the
    configured geometry. Cells outside the geometry remain on the rectangular
    grid with missing values.

    Parameters
    ----------
    data
        Data on one-dimensional ``lat`` and ``lon`` coordinates.
    selection
        Resolved bounds and optional polygon geometry.

    Returns
    -------
    xarray.DataArray
        Spatially selected data with its rectangular grid preserved.
    """
    _validate_rectilinear_grid(data)
    bounds = selection.bounds
    subset = data.sel(
        lat=slice(bounds.south, bounds.north),
        lon=slice(bounds.west, bounds.east),
    )
    if subset.sizes.get("lat", 0) == 0 or subset.sizes.get("lon", 0) == 0:
        raise ValueError("spatial selection produces an empty domain")
    if selection.geometry is None:
        return subset

    longitude_grid, latitude_grid = np.meshgrid(subset["lon"].values, subset["lat"].values)
    mask_values = covers(selection.geometry, points(longitude_grid, latitude_grid))
    mask = xr.DataArray(
        mask_values,
        dims=("lat", "lon"),
        coords={"lat": subset["lat"], "lon": subset["lon"]},
    )
    if not bool(mask.any().item()):
        raise ValueError("SPI shapefile does not contain any grid-cell centers")
    return subset.where(mask)


def _validate_rectilinear_grid(data: xr.DataArray) -> None:
    for coordinate in ("lat", "lon"):
        if coordinate not in data.dims or coordinate not in data.coords:
            raise ValueError(f"spatial data must contain a {coordinate!r} dimension and coordinate")
        if data[coordinate].ndim != 1:
            raise ValueError("spatial clipping requires one-dimensional latitude and longitude")
        values = np.asarray(data[coordinate].values)
        if not np.isfinite(values).all():
            raise ValueError(f"{coordinate} coordinates must be finite")
        if len(np.unique(values)) != len(values):
            raise ValueError(f"{coordinate} coordinates must be unique")
    longitude = np.asarray(data["lon"].values)
    if float(longitude.min()) < -180 or float(longitude.max()) > 180:
        raise ValueError("longitude coordinates must use the -180 to 180 range")
