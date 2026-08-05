from collections.abc import Callable, Sequence
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from riskclima_xhwi.config.settings import CalibrationPolicy, ERA5Settings
from riskclima_xhwi.scripts.cmip6 import make_calibration as cmip6_make_calibration
from riskclima_xhwi.scripts.era5 import concat_months, make_calibration, run_months
from riskclima_xhwi.scripts.era5land import make_calibration as land_make_calibration


def test_all_matching_parts_keeps_previous_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template = str(tmp_path / "parts" / "part-{month}.nc")
    settings = ERA5Settings.model_validate(
        ERA5Settings().model_dump()
        | {
            "part_file_template": template,
            "final_file_template": str(tmp_path / "final.nc"),
        }
    )
    old_part = settings.part_output(1)
    new_part = settings.part_output(2)
    old_part.parent.mkdir(parents=True)
    old_part.touch()
    new_part.touch()
    captured: list[Path] = []

    def capture(
        input_paths: Sequence[Path | str],
        output_path: Path | str,
        **kwargs: object,
    ) -> Path:
        captured.extend(Path(path) for path in input_paths)
        return Path(output_path)

    monkeypatch.setattr(concat_months, "concat_monthly_netcdfs", capture)

    concat_months.run(settings, [new_part])

    assert captured == [old_part, new_part]


def test_current_run_requires_run_all_context(tmp_path: Path) -> None:
    settings = ERA5Settings.model_validate(
        ERA5Settings().model_dump()
        | {
            "concat_input_policy": "current_run",
            "part_file_template": str(tmp_path / "part-{month}.nc"),
            "final_file_template": str(tmp_path / "final.nc"),
        }
    )

    with pytest.raises(ValueError, match="only available through run-all"):
        concat_months.run(settings)


def test_overlapping_runs_keep_one_part_per_month(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = ERA5Settings.model_validate(
        ERA5Settings().model_dump()
        | {"part_file_template": str(tmp_path / "part-{month}.nc"), "netcdf_progress": False}
    )
    calls: list[list[int]] = []

    def compute(path: str, months: Sequence[int], settings: ERA5Settings) -> xr.Dataset:
        calls.append(list(months))
        times = [np.datetime64(f"2020-{month:02d}-01") for month in months]
        return xr.Dataset(
            {
                "xhwi_monthly_accumulated": (
                    ("time", "lat", "lon"),
                    np.ones((len(times), 1, 1), dtype=np.float32),
                )
            },
            coords={"time": times, "lat": [0.0], "lon": [0.0]},
        )

    monkeypatch.setattr(run_months, "compute_era5_monthly_xhwi_torch", compute)

    first = run_months.run(settings, [1, 2])
    second = run_months.run(settings, [2, 3])

    assert calls == [[1, 2], [2, 3]]
    assert first == [settings.part_output(1), settings.part_output(2)]
    assert second == [settings.part_output(2), settings.part_output(3)]
    assert settings.matching_parts() == [
        settings.part_output(1),
        settings.part_output(2),
        settings.part_output(3),
    ]


@pytest.mark.parametrize(
    "command_main",
    [make_calibration.main, land_make_calibration.main, cmip6_make_calibration.main],
)
def test_make_calibration_rejects_in_memory_policy(
    command_main: Callable[[Sequence[str] | None], None],
) -> None:
    with pytest.raises(ValueError, match="make-calibration cannot use"):
        command_main(["--calibration-policy", CalibrationPolicy.IN_MEMORY.value])
