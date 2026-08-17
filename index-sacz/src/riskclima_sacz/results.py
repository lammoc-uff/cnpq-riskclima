"""Consolidate annual SACZ results into daily and monthly time series."""

from pathlib import Path

import numpy as np
import pandas as pd

from riskclima_sacz.config import SACZSettings


class InvalidSACZResultsError(ValueError):
    """Raised when annual or consolidated SACZ results violate their contract."""


def consolidate_daily_results(
    results_directory: Path, regions: tuple[str, ...], output_filename: str
) -> Path:
    """Combine complete annual regional results into one daily time series.

    Parameters
    ----------
    results_directory
        Directory containing numeric year directories with `AB.csv`, `C.csv`,
        and `DE.csv` files.
    regions
        Regional identifiers whose annual files must be consolidated.
    output_filename
        Filename for the consolidated daily CSV.

    Returns
    -------
    pathlib.Path
        Path to the consolidated daily CSV file.

    Raises
    ------
    InvalidSACZResultsError
        If no annual results exist or an annual result is incomplete or invalid.
    """
    if not results_directory.is_dir():
        raise InvalidSACZResultsError(f"SACZ results directory does not exist: {results_directory}")

    year_directories = sorted(
        (path for path in results_directory.iterdir() if path.is_dir() and path.name.isdigit()),
        key=_year_from_path,
    )
    if not year_directories:
        raise InvalidSACZResultsError(f"No annual SACZ results found in {results_directory}")

    annual_results = [_read_annual_results(path, regions) for path in year_directories]
    daily_results = pd.concat(annual_results).sort_index()
    if daily_results.index.duplicated().any():
        raise InvalidSACZResultsError("Consolidated SACZ dates must be unique")

    output_path = results_directory / output_filename
    _write_results(daily_results, output_path, date_format="%Y-%m-%d")
    return output_path


def aggregate_monthly_results(
    results_directory: Path,
    regions: tuple[str, ...],
    daily_filename: str,
    monthly_filename: str,
) -> Path:
    """Sum consolidated daily SACZ indexes by calendar month.

    Parameters
    ----------
    results_directory
        Directory containing the configured consolidated daily CSV.
    regions
        Regional columns required in the consolidated daily CSV.
    daily_filename
        Filename of the consolidated daily CSV.
    monthly_filename
        Filename for the monthly CSV.

    Returns
    -------
    pathlib.Path
        Path to the monthly CSV file.

    Raises
    ------
    InvalidSACZResultsError
        If the consolidated daily result is missing or invalid.
    """
    daily_path = results_directory / daily_filename
    daily_results = _read_consolidated_results(daily_path, regions)
    months = pd.DatetimeIndex(daily_results.index).to_period("M")
    monthly_results = daily_results.groupby(months).sum()
    monthly_results.index = monthly_results.index.astype(str)
    monthly_results.index.name = "time"

    output_path = results_directory / monthly_filename
    _write_results(monthly_results, output_path)
    return output_path


def consolidate_era5_results() -> None:
    """Consolidate available ERA5 annual results using `.env` configuration."""
    settings = SACZSettings()
    consolidate_daily_results(
        settings.path(settings.era5_output_directory),
        settings.regions,
        settings.daily_results_filename,
    )


def aggregate_era5_monthly_results() -> None:
    """Aggregate consolidated ERA5 results using `.env` configuration."""
    settings = SACZSettings()
    aggregate_monthly_results(
        settings.path(settings.era5_output_directory),
        settings.regions,
        settings.daily_results_filename,
        settings.monthly_results_filename,
    )


def consolidate_cmip6_results() -> None:
    """Consolidate configured CMIP6 annual results using `.env` configuration."""
    settings = SACZSettings()
    results_directory = (
        settings.path(settings.cmip6_output_directory)
        / settings.cmip6_source_id
        / settings.cmip6_experiment_id
    )
    consolidate_daily_results(results_directory, settings.regions, settings.daily_results_filename)


def aggregate_cmip6_monthly_results() -> None:
    """Aggregate configured CMIP6 results using `.env` configuration."""
    settings = SACZSettings()
    results_directory = (
        settings.path(settings.cmip6_output_directory)
        / settings.cmip6_source_id
        / settings.cmip6_experiment_id
    )
    aggregate_monthly_results(
        results_directory,
        settings.regions,
        settings.daily_results_filename,
        settings.monthly_results_filename,
    )


def _read_annual_results(year_directory: Path, regions: tuple[str, ...]) -> pd.DataFrame:
    year = int(year_directory.name)
    regional_results = [
        _read_regional_result(year_directory / f"{region}.csv", region, year) for region in regions
    ]
    reference_dates = regional_results[0].index
    if any(not result.index.equals(reference_dates) for result in regional_results[1:]):
        raise InvalidSACZResultsError(f"Regional dates are not aligned in {year_directory}")
    return pd.concat(regional_results, axis=1)


def _year_from_path(path: Path) -> int:
    return int(path.name)


def _read_regional_result(path: Path, region: str, year: int) -> pd.Series:
    if not path.is_file():
        raise InvalidSACZResultsError(f"Missing regional result: {path}")

    table = pd.read_csv(path)
    value_columns = [column for column in table.columns if column != "time"]
    if "time" not in table.columns or len(value_columns) != 1:
        raise InvalidSACZResultsError(f"Expected time and one value column in {path}")

    dates = pd.to_datetime(table["time"], errors="raise").dt.normalize()
    if dates.duplicated().any():
        raise InvalidSACZResultsError(f"Dates must be unique in {path}")
    if not dates.dt.year.eq(year).all():
        raise InvalidSACZResultsError(f"Dates in {path} must belong to {year}")

    values = pd.to_numeric(table[value_columns[0]], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise InvalidSACZResultsError(f"Values must be finite in {path}")
    if ((values < 0.0) | (values > 1.0)).any():
        raise InvalidSACZResultsError(f"Values must be within [0, 1] in {path}")

    return pd.Series(values, index=pd.DatetimeIndex(dates), name=region)


def _read_consolidated_results(path: Path, regions: tuple[str, ...]) -> pd.DataFrame:
    if not path.is_file():
        raise InvalidSACZResultsError(f"Missing consolidated daily result: {path}")

    table = pd.read_csv(path)
    expected_columns = ["time", *regions]
    if table.columns.tolist() != expected_columns:
        raise InvalidSACZResultsError(f"Expected columns {expected_columns} in {path}")

    dates = pd.to_datetime(table.pop("time"), errors="raise")
    if dates.duplicated().any() or not dates.is_monotonic_increasing:
        raise InvalidSACZResultsError(f"Dates must be unique and ordered in {path}")
    values = table.to_numpy(dtype=float)
    if not np.isfinite(values).all() or ((values < 0.0) | (values > 1.0)).any():
        raise InvalidSACZResultsError(f"Values must be finite and within [0, 1] in {path}")

    table.index = pd.DatetimeIndex(dates, name="time")
    return table


def _write_results(
    results: pd.DataFrame, output_path: Path, *, date_format: str | None = None
) -> None:
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    results.to_csv(temporary_path, date_format=date_format)
    temporary_path.replace(output_path)
