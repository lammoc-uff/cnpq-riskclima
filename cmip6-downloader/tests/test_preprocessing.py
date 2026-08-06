"""Synthetic calendar and coordinate preprocessing tests."""

from datetime import date

import cftime
import pandas as pd
import pytest
import xarray as xr

from src.config import CurvilinearPolicy, Settings
from src.preprocessing import (
    normalize_coordinates,
    period_for_experiment,
    preprocess_time,
    subset_domain,
)


def _with(settings: Settings, **updates: str | bool | float | date) -> Settings:
    values = settings.model_dump()
    values.update(updates)
    return Settings.model_validate(values)


def test_historical_and_future_periods(settings: Settings) -> None:
    configured = _with(
        settings,
        historical_start=date(1950, 1, 1),
        historical_end=date(2014, 12, 31),
    )
    assert period_for_experiment(configured, "historical") == (
        date(1950, 1, 1),
        date(2014, 12, 31),
    )
    assert period_for_experiment(configured, "ssp245") == (
        date(2015, 1, 1),
        date(2050, 12, 31),
    )


def test_historical_period_is_applied(settings: Settings) -> None:
    configured = _with(
        settings,
        historical_start=date(2000, 1, 2),
        historical_end=date(2000, 1, 3),
    )
    dataset = xr.Dataset(
        {"tas": ("time", [1.0, 2.0, 3.0, 4.0])},
        coords={"time": xr.date_range("2000-01-01", periods=4, freq="D")},
    )
    result = preprocess_time(dataset, configured, "historical")
    assert result.sizes["time"] == 2


@pytest.mark.parametrize(("drop_duplicates", "expected_size"), [(True, 1), (False, 2)])
def test_duplicate_time_policy(
    drop_duplicates: bool,
    expected_size: int,
    settings: Settings,
) -> None:
    configured = _with(settings, drop_duplicate_times=drop_duplicates)
    dataset = xr.Dataset(
        {"tas": ("time", [1.0, 2.0])},
        coords={"time": pd.to_datetime(["2000-01-01", "2000-01-01"])},
    )
    assert preprocess_time(dataset, configured, "historical").sizes["time"] == expected_size


@pytest.mark.parametrize("calendar", ["noleap", "360_day"])
def test_calendar_conversion(calendar: str, settings: Settings) -> None:
    time = xr.date_range("2015-01-01", periods=4, freq="D", calendar=calendar, use_cftime=True)
    dataset = xr.Dataset({"tas": ("time", [1.0, 2.0, 3.0, 4.0])}, coords={"time": time})
    result = preprocess_time(dataset, settings, "ssp245")
    assert isinstance(result.indexes["time"], pd.DatetimeIndex)
    assert not isinstance(result.indexes["time"][0], cftime.datetime)


def test_coordinate_aliases_longitude_and_domain(settings: Settings) -> None:
    dataset = xr.Dataset(
        {"tas": (("latitude", "longitude"), [[1.0, 2.0], [3.0, 4.0]])},
        coords={"latitude": [-80.0, 0.0], "longitude": [240.0, 350.0]},
    )
    normalized = normalize_coordinates(dataset)
    result = subset_domain(normalized, settings, "tas")
    assert set(result.coords) == {"lat", "lon"}
    assert result["lat"].values.tolist() == [0.0]
    assert result["lon"].values.tolist() == [-120.0, -10.0]


def test_tos_and_curvilinear_policies(settings: Settings) -> None:
    dataset = xr.Dataset(
        {"tos": (("y", "x"), [[1.0, 2.0]])},
        coords={"lat": (("y", "x"), [[-40.0, -30.0]]), "lon": (("y", "x"), [[-50.0, -40.0]])},
    )
    assert subset_domain(dataset, settings, "tos").identical(dataset)
    reject = _with(settings, curvilinear_policy=CurvilinearPolicy.REJECT.value)
    with pytest.raises(ValueError, match="curvilinear"):
        subset_domain(dataset.rename(tos="tas"), reject, "tas")
