from pathlib import Path

import pandas as pd
import pytest

from riskclima_sacz.results import (
    InvalidSACZResultsError,
    aggregate_monthly_results,
    consolidate_daily_results,
)

REGIONS = ("AB", "C", "DE")
DAILY_RESULTS_FILENAME = "daily.csv"
MONTHLY_RESULTS_FILENAME = "monthly.csv"


def test_consolidates_daily_results_and_sums_calendar_months(tmp_path: Path) -> None:
    _write_annual_results(
        tmp_path,
        2001,
        ["2001-01-01 11:00:00"],
        {"AB": [0.4], "C": [0.5], "DE": [0.6]},
    )
    _write_annual_results(
        tmp_path,
        2000,
        ["2000-01-30 11:00:00", "2000-01-31 11:00:00", "2000-02-01 11:00:00"],
        {"AB": [0.1, 0.2, 0.3], "C": [0.2, 0.3, 0.4], "DE": [0.3, 0.4, 0.5]},
    )

    daily_path = consolidate_daily_results(tmp_path, REGIONS, DAILY_RESULTS_FILENAME)
    monthly_path = aggregate_monthly_results(
        tmp_path,
        REGIONS,
        DAILY_RESULTS_FILENAME,
        MONTHLY_RESULTS_FILENAME,
    )

    assert daily_path == tmp_path / DAILY_RESULTS_FILENAME
    assert pd.read_csv(daily_path).to_dict(orient="list") == {
        "time": ["2000-01-30", "2000-01-31", "2000-02-01", "2001-01-01"],
        "AB": [0.1, 0.2, 0.3, 0.4],
        "C": [0.2, 0.3, 0.4, 0.5],
        "DE": [0.3, 0.4, 0.5, 0.6],
    }
    assert monthly_path == tmp_path / MONTHLY_RESULTS_FILENAME
    assert pd.read_csv(monthly_path).to_dict(orient="list") == {
        "time": ["2000-01", "2000-02", "2001-01"],
        "AB": pytest.approx([0.3, 0.3, 0.4]),
        "C": pytest.approx([0.5, 0.4, 0.5]),
        "DE": pytest.approx([0.7, 0.5, 0.6]),
    }


def test_consolidation_rejects_incomplete_year(tmp_path: Path) -> None:
    year_directory = tmp_path / "2001"
    year_directory.mkdir()
    pd.DataFrame({"time": ["2001-01-01"], "AB": [0.5]}).to_csv(
        year_directory / "AB.csv", index=False
    )

    with pytest.raises(InvalidSACZResultsError, match="Missing regional result"):
        consolidate_daily_results(tmp_path, REGIONS, DAILY_RESULTS_FILENAME)


def test_consolidation_rejects_misaligned_regional_dates(tmp_path: Path) -> None:
    _write_annual_results(
        tmp_path,
        2001,
        ["2001-01-01"],
        {"AB": [0.4], "C": [0.5], "DE": [0.6]},
    )
    pd.DataFrame({"time": ["2001-01-02"], "C": [0.5]}).to_csv(tmp_path / "2001/C.csv", index=False)

    with pytest.raises(InvalidSACZResultsError, match="Regional dates are not aligned"):
        consolidate_daily_results(tmp_path, REGIONS, DAILY_RESULTS_FILENAME)


def _write_annual_results(
    results_directory: Path,
    year: int,
    dates: list[str],
    values_by_region: dict[str, list[float]],
) -> None:
    year_directory = results_directory / str(year)
    year_directory.mkdir()
    for region, values in values_by_region.items():
        pd.DataFrame({"time": dates, region: values}).to_csv(
            year_directory / f"{region}.csv", index=False
        )
