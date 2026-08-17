from pathlib import Path
from shutil import copytree

import pandas as pd
import pytest

from riskclima_sacz.config import SACZSettings
from riskclima_sacz.main_sacz_index_cmip6 import (
    configure as configure_cmip6,
)
from riskclima_sacz.main_sacz_index_cmip6 import (
    process_year as process_cmip6_year,
)
from riskclima_sacz.main_sacz_index_era5 import configure as configure_era5
from riskclima_sacz.main_sacz_index_era5 import process_year as process_era5_year
from riskclima_sacz.model import logistic_probability

PROJECT_DIRECTORY = Path(__file__).parents[1]
FIXTURES_DIRECTORY = PROJECT_DIRECTORY / "tests/fixtures/regression"
REGIONS = ("AB", "C", "DE")


def test_era5_index_matches_original_results(tmp_path: Path) -> None:
    fixture_directory = FIXTURES_DIRECTORY / "era5"
    copytree(fixture_directory / "input", tmp_path / "input/2001")
    settings = SACZSettings(
        base_directory=tmp_path,
        coefficients_directory=PROJECT_DIRECTORY / "coefs",
        era5_year=2001,
        era5_input_directory=Path("input"),
        era5_intermediates_directory=Path("intermediates"),
        era5_output_directory=Path("results"),
    )

    configure_era5(settings)
    process_era5_year(2001, "era5")

    actual = _read_regional_results(tmp_path / "results/2001")
    expected = pd.read_csv(fixture_directory / "expected.csv", parse_dates=["time"]).set_index(
        "time"
    )
    pd.testing.assert_frame_equal(actual, expected, rtol=1e-12, atol=1e-12)


def test_cmip6_index_matches_original_results(tmp_path: Path) -> None:
    fixture_directory = FIXTURES_DIRECTORY / "cmip6"
    source_id = "BCC-CSM2-MR"
    experiment_id = "historical"
    copytree(
        fixture_directory / "input",
        tmp_path / f"input/{source_id}/{experiment_id}/1968",
    )
    settings = SACZSettings(
        base_directory=tmp_path,
        coefficients_directory=PROJECT_DIRECTORY / "coefs",
        cmip6_input_directory=Path("input"),
        cmip6_intermediates_directory=Path("intermediates"),
        cmip6_output_directory=Path("results"),
        cmip6_source_id=source_id,
        cmip6_experiment_id=experiment_id,
        cmip6_start_year=1968,
        cmip6_end_year=1968,
    )

    configure_cmip6(settings)
    process_cmip6_year(1968, source_id, experiment_id)

    actual = _read_regional_results(tmp_path / f"results/{source_id}/{experiment_id}/1968")
    expected = pd.read_csv(fixture_directory / "expected.csv", parse_dates=["time"]).set_index(
        "time"
    )
    pd.testing.assert_frame_equal(actual, expected, rtol=1e-12, atol=1e-12)


@pytest.mark.parametrize(
    ("score", "expected"),
    ((-1_000.0, 0.0), (0.0, 0.5), (1_000.0, 1.0)),
)
def test_logistic_probability_is_stable(score: float, expected: float) -> None:
    assert logistic_probability(score) == expected


def _read_regional_results(results_directory: Path) -> pd.DataFrame:
    regional_results = []
    for region in REGIONS:
        table = pd.read_csv(results_directory / f"{region}.csv", parse_dates=["time"])
        value_column = next(column for column in table.columns if column != "time")
        regional_results.append(table.set_index("time")[value_column].rename(region))
    return pd.concat(regional_results, axis=1)
