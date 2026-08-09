"""Time and coordinate preprocessing for CMIP6 datasets."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex

from src.config import CurvilinearPolicy, Settings

LOGGER = logging.getLogger(__name__)


def period_for_experiment(
    settings: Settings, experiment_id: str
) -> tuple[date | None, date | None]:
    """Return configured temporal bounds for an experiment."""
    if experiment_id in settings.future_experiments:
        return settings.future_start, settings.future_end
    if experiment_id in settings.historical_experiments:
        return settings.historical_start, settings.historical_end
    return None, None


def preprocess_time(ds: xr.Dataset, settings: Settings, experiment_id: str) -> xr.Dataset:
    """Sort, convert, deduplicate, and subset a dataset's time coordinate."""
    if "time" not in ds.coords:
        raise ValueError("dataset does not contain a time coordinate")
    if not ds.indexes["time"].is_monotonic_increasing:
        ds = ds.sortby("time")
    if settings.calendar_conversion and isinstance(ds.indexes["time"], CFTimeIndex):
        ds = ds.convert_calendar(
            settings.target_calendar,
            align_on=settings.calendar_align_on,
        )
    if settings.convert_datetime_index and isinstance(ds.indexes["time"], CFTimeIndex):
        try:
            ds["time"] = ds.indexes["time"].to_datetimeindex(time_unit="ns")
        except (ValueError, OverflowError) as error:
            raise ValueError(
                "converted calendar cannot be represented by a datetime index"
            ) from error
    if settings.drop_duplicate_times:
        duplicated = pd.Index(ds.indexes["time"]).duplicated()
        if duplicated.any():
            ds = ds.isel(time=~duplicated)
    start, end = period_for_experiment(settings, experiment_id)
    if start is not None or end is not None:
        ds = ds.sel(
            time=slice(start.isoformat() if start else None, end.isoformat() if end else None)
        )
    return ds


def normalize_coordinates(ds: xr.Dataset) -> xr.Dataset:
    """Rename coordinate aliases and normalize longitude to [-180, 180]."""
    aliases = {
        name: canonical
        for name, canonical in (("latitude", "lat"), ("longitude", "lon"))
        if name in ds.coords and canonical not in ds.coords
    }
    if aliases:
        ds = ds.rename(aliases)
    if "lat" not in ds.coords or "lon" not in ds.coords:
        raise ValueError("dataset must provide lat/lon or latitude/longitude coordinates")
    longitude = ds["lon"]
    ds = ds.assign_coords(lon=((longitude + 180) % 360) - 180)
    if ds["lon"].ndim == 1:
        ds = ds.sortby("lon")
    if ds["lat"].ndim == 1:
        ds = ds.sortby("lat")
    return ds


def subset_domain(ds: xr.Dataset, settings: Settings, variable_id: str) -> xr.Dataset:
    """Apply the configured rectilinear domain or curvilinear policy."""
    if variable_id in settings.excluded_variables or not settings.spatial_subset:
        return ds
    if ds["lat"].ndim != 1 or ds["lon"].ndim != 1:
        if settings.curvilinear_policy is CurvilinearPolicy.REJECT:
            raise ValueError("curvilinear coordinates are rejected by configuration")
        LOGGER.info("Keeping global curvilinear dataset for %s", variable_id)
        return ds
    return ds.sel(
        lat=slice(settings.latitude_min, settings.latitude_max),
        lon=slice(settings.longitude_min, settings.longitude_max),
    )


def preprocess_dataset(
    ds: xr.Dataset, settings: Settings, experiment_id: str, variable_id: str
) -> xr.Dataset:
    """Apply all configured preprocessing steps."""
    ds = preprocess_time(ds, settings, experiment_id)
    ds = normalize_coordinates(ds)
    return subset_domain(ds, settings, variable_id)
