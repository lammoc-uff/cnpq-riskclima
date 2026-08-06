"""Provider mapper and output-task tests."""

from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import xarray as xr
from pytest import MonkeyPatch

from src.config import Settings
from src.downloader import CMIP6Downloader, member_output_tasks, provider_mapper
from src.filters import create_group_key


def test_provider_mapper_uses_isolated_options(
    monkeypatch: MonkeyPatch,
    settings: Settings,
) -> None:
    aws_filesystem = Mock()
    aws_mapper: dict[str, bytes] = {}
    aws_filesystem.get_mapper.return_value = aws_mapper
    google_filesystem = Mock()
    google_mapper: dict[str, bytes] = {}
    google_filesystem.get_mapper.return_value = google_mapper
    aws_constructor = Mock(return_value=aws_filesystem)
    google_constructor = Mock(return_value=google_filesystem)
    monkeypatch.setattr("src.downloader.s3fs.S3FileSystem", aws_constructor)
    monkeypatch.setattr("src.downloader.gcsfs.GCSFileSystem", google_constructor)

    provider_mapper("s3://bucket/a", "aws", settings)
    provider_mapper("gs://bucket/a", "google", settings)

    aws_constructor.assert_called_once_with(anon=True)
    google_constructor.assert_called_once_with(token="anon")
    aws_filesystem.get_mapper.assert_called_once_with("s3://bucket/a")
    google_filesystem.get_mapper.assert_called_once_with("gs://bucket/a")


def test_remote_open_uses_configured_chunks(
    monkeypatch: MonkeyPatch,
    settings: Settings,
) -> None:
    values = settings.model_dump()
    values["open_chunks"] = {"lat": 8}
    configured = Settings.model_validate(values)
    downloader = CMIP6Downloader(configured)
    mapper: dict[str, bytes] = {}
    monkeypatch.setattr("src.downloader.provider_mapper", Mock(return_value=mapper))
    open_zarr = Mock(return_value=xr.Dataset())
    monkeypatch.setattr("src.downloader.xr.open_zarr", open_zarr)

    downloader._open_store("s3://bucket/a", "aws")

    open_zarr.assert_called_once_with(
        mapper,
        consolidated=configured.remote_consolidated,
        chunks={"lat": 8},
    )


def test_segments_create_one_output_task(
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    segments = pd.concat(
        [catalog, catalog.assign(version="v20240101", zstore="s3://bucket/b.zarr")],
        ignore_index=True,
    )
    segments["group_key"] = create_group_key(segments)
    tasks = member_output_tasks(segments, tmp_path, settings)
    assert len(tasks) == 1
    assert len(next(iter(tasks.values()))) == 2


def test_member_task_uses_configured_template(
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    values = settings.model_dump()
    values["member_store_template"] = "cmip-{member_id}.zarr"
    configured = Settings.model_validate(values)
    frame = catalog.assign(group_key=create_group_key(catalog))
    task = next(iter(member_output_tasks(frame, tmp_path, configured)))
    assert task.name == "cmip-r1i1p1f1.zarr"


def test_member_task_rejects_dynamic_group_artifact_collision(
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    values = settings.model_dump()
    values["member_store_template"] = "{member_id}.zarr"
    configured = Settings.model_validate(values)
    frame = catalog.assign(member_id="ensemble_all")
    frame["group_key"] = create_group_key(frame)

    with pytest.raises(ValueError, match=r"member store filename collides.*ensemble_all\.zarr"):
        member_output_tasks(frame, tmp_path, configured)


def _configured_downloads(settings: Settings, path: Path) -> Settings:
    values = settings.model_dump()
    values["downloads_dir"] = path
    values["spatial_subset"] = False
    return Settings.model_validate(values)


def _remote_dataset(times: list[str], values: list[float]) -> xr.Dataset:
    return xr.Dataset(
        {"tas": ("time", values)},
        coords={
            "time": pd.to_datetime(times),
            "lat": ("lat", [0.0]),
            "lon": ("lon", [0.0]),
        },
    )


def test_process_member_uses_latest_version_on_overlap(
    monkeypatch: MonkeyPatch,
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    configured = _configured_downloads(settings, tmp_path)
    downloader = CMIP6Downloader(configured)
    rows = pd.concat(
        [
            catalog.assign(version="v2", zstore="s3://bucket/new.zarr"),
            catalog.assign(version="v1", zstore="s3://bucket/old.zarr"),
        ],
        ignore_index=True,
    )
    datasets = {
        "s3://bucket/new.zarr": _remote_dataset(["2000-01-02"], [20.0]),
        "s3://bucket/old.zarr": _remote_dataset(
            ["2000-01-01", "2000-01-02", "2000-01-03"],
            [1.0, 2.0, 3.0],
        ),
    }

    def open_dataset(row: pd.Series) -> xr.Dataset:
        return datasets[str(row["zstore"])]

    monkeypatch.setattr(downloader, "_open_dataset", open_dataset)
    output = tmp_path / "member-r1.zarr"
    result = downloader.process_member(rows, output)
    assert result["status"] == "Success"
    with xr.open_zarr(output) as written:
        assert written["tas"].values.tolist() == [1.0, 20.0, 3.0]


def test_primary_open_failure_uses_alternate(
    monkeypatch: MonkeyPatch,
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    downloader = CMIP6Downloader(settings)
    expected = _remote_dataset(["2000-01-01"], [1.0])
    open_store = Mock(side_effect=[OSError("AWS failed"), expected])
    monkeypatch.setattr(downloader, "_open_store", open_store)
    row = catalog.assign(
        provider="aws",
        alternate_provider="google",
        alternate_zstore="gs://bucket/tas.zarr",
    ).iloc[0]
    assert downloader._open_dataset(row) is expected
    assert open_store.call_args_list[1].args == ("gs://bucket/tas.zarr", "google")


def test_group_member_error_prevents_ensemble(
    monkeypatch: MonkeyPatch,
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    configured = _configured_downloads(settings, tmp_path)
    downloader = CMIP6Downloader(configured)
    frame = catalog.assign(provider="aws")
    frame["group_key"] = create_group_key(frame)

    def fail_member(rows: pd.DataFrame, path: Path) -> dict[str, str]:
        return {"file": str(path), "status": "Error", "message": "failed"}

    monkeypatch.setattr(downloader, "process_member", fail_member)
    ensemble = Mock()
    monkeypatch.setattr("src.downloader.build_ensemble", ensemble)
    logs = downloader.process_group(frame)
    assert logs[0]["status"] == "Error"
    ensemble.assert_not_called()


def test_group_rejects_source_directory_colliding_with_global_log(
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    configured = _configured_downloads(settings, tmp_path)
    downloader = CMIP6Downloader(configured)
    frame = catalog.assign(source_id=configured.global_log_filename)
    frame["group_key"] = create_group_key(frame)

    with pytest.raises(ValueError, match="source directory collides with global log"):
        downloader.process_group(frame)
    assert not (tmp_path / configured.global_log_filename).exists()


def test_empty_catalog_run_writes_empty_success_log(
    settings: Settings,
    tmp_path: Path,
) -> None:
    preferred = tmp_path / "preferred.csv"
    pd.DataFrame(
        columns=[
            "source_id",
            "experiment_id",
            "table_id",
            "variable_id",
            "grid_label",
            "member_id",
            "version",
            "zstore",
            "provider",
        ]
    ).to_csv(preferred, index=False)
    values = settings.model_dump()
    values.update({"downloads_dir": tmp_path / "downloads", "preferred_catalog_path": preferred})
    result = CMIP6Downloader(Settings.model_validate(values)).run()
    assert result.empty
    assert list(result.columns) == ["file", "status", "message"]
