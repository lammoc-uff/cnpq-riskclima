from pathlib import Path

import xarray as xr

from riskclima_spi.config import CMIP6Settings
from riskclima_spi.pipeline import (
    SPISourceMetadata,
    build_output_dataset,
    calculate_spi,
    open_precipitation_dataset,
    write_output,
)


def prepare_cmip6_monthly_precipitation(
    dataset: xr.Dataset, settings: CMIP6Settings
) -> xr.DataArray:
    """Convert daily CMIP6 precipitation flux into monthly accumulation.

    Parameters
    ----------
    dataset
        Preprocessed CMIP6 dataset containing daily precipitation flux.
    settings
        CMIP6 variable and dimension names.

    Returns
    -------
    xarray.DataArray
        Monthly accumulated precipitation in millimetres.
    """
    variable = settings.cmip6_precipitation_variable
    dimensions = (
        settings.cmip6_time_dimension,
        settings.cmip6_latitude_dimension,
        settings.cmip6_longitude_dimension,
    )
    if variable not in dataset:
        raise ValueError(f"precipitation variable {variable!r} is not present")
    missing_dimensions = [
        dimension for dimension in dimensions if dimension not in dataset[variable].dims
    ]
    if missing_dimensions:
        raise ValueError(f"precipitation variable is missing dimensions: {missing_dimensions}")
    units = dataset[variable].attrs.get("units", "")
    if units not in {"kg m-2 s-1", "kg m**-2 s**-1", "mm s-1"}:
        raise ValueError("precipitation units must be a daily flux in kg m-2 s-1 or mm s-1")
    precipitation = dataset[variable].rename(
        {
            settings.cmip6_time_dimension: "time",
            settings.cmip6_latitude_dimension: "lat",
            settings.cmip6_longitude_dimension: "lon",
        }
    )
    precipitation = precipitation.sortby(["time", "lat", "lon"])
    _validate_complete_daily_series(precipitation)
    precipitation = precipitation * 86400
    precipitation.attrs = {"units": "mm day-1"}
    monthly = precipitation.resample(time="MS").sum(min_count=1)
    return monthly.rename("pr").assign_attrs(units="mm month-1")


def run_cmip6(settings: CMIP6Settings) -> Path:
    """Run SPI for one configured CMIP6 experiment.

    Parameters
    ----------
    settings
        CMIP6 input, SPI, output, and metadata configuration.

    Returns
    -------
    pathlib.Path
        Written SPI NetCDF path.
    """
    with (
        open_precipitation_dataset(settings.cmip6_calibration_input_file) as calibration_dataset,
        open_precipitation_dataset(settings.cmip6_input_file) as input_dataset,
    ):
        calibration_monthly = prepare_cmip6_monthly_precipitation(calibration_dataset, settings)
        input_monthly = prepare_cmip6_monthly_precipitation(input_dataset, settings)
        _validate_exact_spatial_grid(calibration_monthly, input_monthly)
        spi = calculate_spi(input_monthly, calibration_monthly, settings)
        output = build_output_dataset(
            spi,
            settings,
            source_metadata=SPISourceMetadata(
                title=f"Standardized Precipitation Index for CMIP6 {settings.cmip6_experiment}",
                source=f"CMIP6 experiment {settings.cmip6_experiment} daily precipitation",
                keywords="spi, CMIP6, climate projection, RiskClima",
                input_variables=settings.cmip6_precipitation_variable,
                input_frequency="daily",
                precipitation_conversion=(
                    "Daily precipitation flux in kg m-2 s-1 or equivalent mm s-1 was "
                    "multiplied by 86400 to obtain mm day-1. Daily values were summed "
                    "into monthly-start intervals with "
                    "resample(time='MS').sum(min_count=1), producing monthly accumulated "
                    "precipitation in mm month-1."
                ),
            ),
        )
        output.attrs.update(
            dataset_id="CMIP6",
            source_id="cmip6",
            model_id=settings.cmip6_model,
            experiment_id=settings.cmip6_experiment,
            member_id=settings.cmip6_member,
            grid_label=settings.cmip6_grid,
        )
        return write_output(output, settings.output_path(), settings)


def _validate_exact_spatial_grid(calibration: xr.DataArray, application: xr.DataArray) -> None:
    if not calibration["lat"].equals(application["lat"]) or not calibration["lon"].equals(
        application["lon"]
    ):
        raise ValueError(
            "CMIP6 calibration and application data must have exactly equal latitude "
            "and longitude grids"
        )


def _validate_complete_daily_series(precipitation: xr.DataArray) -> None:
    years = precipitation["time"].dt.year.values.tolist()
    months = precipitation["time"].dt.month.values.tolist()
    days = precipitation["time"].dt.day.values.tolist()
    days_in_month = precipitation["time"].dt.days_in_month.values.tolist()
    actual = [
        (int(year), int(month), int(day))
        for year, month, day in zip(years, months, days, strict=True)
    ]
    if actual != sorted(set(actual)):
        raise ValueError("CMIP6 precipitation timestamps must be unique and ordered daily")
    counts: dict[tuple[int, int], int] = {}
    expected_counts: dict[tuple[int, int], int] = {}
    for year, month, month_days in zip(years, months, days_in_month, strict=True):
        key = int(year), int(month)
        counts[key] = counts.get(key, 0) + 1
        expected_counts[key] = int(month_days)
    if counts != expected_counts:
        raise ValueError("CMIP6 precipitation must contain every day of each represented month")
