"""Zarr writing, existing-policy, ensemble, and cleanup tests."""

import logging
import shutil
from pathlib import Path
from typing import Literal

import numpy as np
import pytest
import xarray as xr
import zarr
from pytest import MonkeyPatch

from src.config import EnsembleMode, ExistingPolicy, Settings
from src.writer import (
    _cleanup_member_stores,
    _ensemble_chunks,
    build_ensemble,
    save_dataset_in_blocks,
    validate_store,
)


def _settings(settings: Settings, **updates: str | bool | int) -> Settings:
    values = settings.model_dump()
    values.update(updates)
    return Settings.model_validate(values)


def _dataset(values: list[float]) -> xr.Dataset:
    return xr.Dataset(
        {"tas": ("time", values, {"units": "K"})},
        coords={"time": xr.date_range("2000-01-01", periods=len(values), freq="D")},
        attrs={"source": "test"},
    )


def _write_member(path: Path, values: list[float], settings: Settings) -> None:
    save_dataset_in_blocks(_dataset(values), path, "tas", settings)


def test_writer_is_zarr_v2_and_consolidated(tmp_path: Path, settings: Settings) -> None:
    path = tmp_path / "member-r1.zarr"
    _write_member(path, [1.0, 2.0], settings)
    assert (path / ".zgroup").exists()
    assert (path / ".zmetadata").exists()
    assert zarr.open_group(path).attrs.asdict()["source"] == "test"


def test_ensemble_chunks_use_dimension_time_and_default_settings(settings: Settings) -> None:
    values = settings.model_dump()
    values.update(
        {
            "time_chunk_size": 3,
            "ensemble_dimension_chunks": {"member": 2, "lat": 4},
            "ensemble_default_chunk_size": 5,
        }
    )
    configured = Settings.model_validate(values)
    dataset = xr.Dataset(
        {"tas": (("member", "time", "lat", "lon"), np.arange(2 * 7 * 6 * 8).reshape(2, 7, 6, 8))}
    )
    assert _ensemble_chunks(dataset, configured) == {
        "member": 2,
        "time": 3,
        "lat": 4,
        "lon": 5,
    }


def test_existing_policies_and_invalid_skip(tmp_path: Path, settings: Settings) -> None:
    path = tmp_path / "member-r1.zarr"
    _write_member(path, [1.0], settings)
    assert not save_dataset_in_blocks(_dataset([2.0]), path, "tas", settings)
    fail = _settings(settings, existing_policy=ExistingPolicy.FAIL.value)
    with pytest.raises(FileExistsError):
        save_dataset_in_blocks(_dataset([2.0]), path, "tas", fail)
    overwrite = _settings(settings, existing_policy=ExistingPolicy.OVERWRITE.value)
    assert save_dataset_in_blocks(_dataset([2.0]), path, "tas", overwrite)
    invalid = tmp_path / "member-invalid.zarr"
    invalid.mkdir()
    with pytest.raises(ValueError, match="invalid"):
        save_dataset_in_blocks(_dataset([1.0]), invalid, "tas", settings)
    with pytest.raises(ValueError, match="unexpected time size"):
        save_dataset_in_blocks(_dataset([1.0, 2.0]), path, "tas", settings)


@pytest.mark.parametrize("cleanup", [True, False])
def test_ensemble_and_multiple_member_cleanup(
    cleanup: bool,
    tmp_path: Path,
    settings: Settings,
) -> None:
    configured = _settings(
        settings,
        cleanup_members=cleanup,
        ensemble_mode=EnsembleMode.BOTH.value,
    )
    members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    _write_member(members[0], [1.0, 3.0], configured)
    _write_member(members[1], [3.0, 5.0], configured)
    assert build_ensemble({"r1": members[0], "r2": members[1]}, tmp_path, "tas", configured)
    assert (tmp_path / configured.ensemble_all_filename).exists()
    assert (tmp_path / configured.ensemble_mean_filename).exists()
    with xr.open_zarr(tmp_path / configured.ensemble_mean_filename) as mean:
        assert mean["tas"].values.tolist() == [2.0, 4.0]
        assert mean["tas"].attrs["units"] == "K"
        assert mean.attrs["source_member_ids"] == ["r1", "r2"]
        assert mean.attrs["source_member_count"] == 2
        assert mean.attrs["source_time_count"] == 2
    assert all(path.exists() is not cleanup for path in members)


def test_single_member_is_always_preserved(tmp_path: Path, settings: Settings) -> None:
    member = tmp_path / "member-r1.zarr"
    _write_member(member, [1.0], settings)
    assert not build_ensemble({"r1": member}, tmp_path, "tas", settings)
    assert member.exists()


@pytest.mark.parametrize(
    ("mode", "stack_exists", "mean_exists"),
    [
        (EnsembleMode.NONE, False, False),
        (EnsembleMode.STACK, True, False),
        (EnsembleMode.MEAN, False, True),
    ],
)
def test_ensemble_modes(
    mode: EnsembleMode,
    stack_exists: bool,
    mean_exists: bool,
    tmp_path: Path,
    settings: Settings,
) -> None:
    configured = _settings(settings, ensemble_mode=mode.value, cleanup_members=False)
    members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    _write_member(members[0], [1.0], configured)
    _write_member(members[1], [3.0], configured)
    build_ensemble({"r1": members[0], "r2": members[1]}, tmp_path, "tas", configured)
    assert (tmp_path / configured.ensemble_all_filename).exists() is stack_exists
    assert (tmp_path / configured.ensemble_mean_filename).exists() is mean_exists


def test_failed_block_write_preserves_old_destination_and_removes_partial(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    settings: Settings,
) -> None:
    path = tmp_path / "member-r1.zarr"
    _write_member(path, [9.0, 9.0], settings)
    configured = _settings(
        settings,
        existing_policy=ExistingPolicy.OVERWRITE.value,
        time_chunk_size=1,
    )
    original = xr.Dataset.to_zarr
    calls = 0

    def fail_second_block(
        dataset: xr.Dataset,
        store: Path,
        *,
        mode: Literal["a", "w"],
        append_dim: Literal["time"] | None,
        consolidated: bool,
        zarr_format: int,
    ) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated block failure")
        return original(
            dataset,
            store,
            mode=mode,
            append_dim=append_dim,
            consolidated=consolidated,
            zarr_format=zarr_format,
        )

    monkeypatch.setattr(xr.Dataset, "to_zarr", fail_second_block)
    with pytest.raises(RuntimeError, match="simulated"):
        save_dataset_in_blocks(_dataset([1.0, 2.0]), path, "tas", configured)
    with xr.open_zarr(path) as existing:
        assert existing["tas"].values.tolist() == [9.0, 9.0]
    assert not list(tmp_path.glob("*.partial-*"))
    assert not list(tmp_path.glob("*.backup-*"))


def test_output_lock_rejects_concurrent_writer(tmp_path: Path, settings: Settings) -> None:
    path = tmp_path / "member-r1.zarr"
    lock = tmp_path / ".member-r1.zarr.lock"
    lock.mkdir()
    with pytest.raises(FileExistsError, match="locked"):
        save_dataset_in_blocks(_dataset([1.0]), path, "tas", settings)
    assert lock.exists()


def test_partial_store_cannot_validate(tmp_path: Path) -> None:
    partial = tmp_path / "member-r1.zarr.partial-deadbeef"
    partial.mkdir()
    with pytest.raises(ValueError, match="partial"):
        validate_store(partial, "tas")


def test_stale_existing_ensemble_fails_without_cleanup(
    tmp_path: Path,
    settings: Settings,
) -> None:
    configured = _settings(settings, cleanup_members=False, ensemble_mode=EnsembleMode.STACK.value)
    first_members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    _write_member(first_members[0], [1.0], configured)
    _write_member(first_members[1], [2.0], configured)
    assert build_ensemble(
        {"r1": first_members[0], "r2": first_members[1]}, tmp_path, "tas", configured
    )

    replacement = tmp_path / "member-r3.zarr"
    _write_member(replacement, [3.0], configured)
    current_members = [first_members[0], replacement]
    with pytest.raises(ValueError, match="stale member provenance"):
        build_ensemble(
            {"r1": current_members[0], "r3": current_members[1]}, tmp_path, "tas", configured
        )
    assert all(path.exists() for path in current_members)


def test_valid_existing_ensemble_skip_allows_cleanup(
    tmp_path: Path,
    settings: Settings,
) -> None:
    preserve = _settings(settings, cleanup_members=False, ensemble_mode=EnsembleMode.STACK.value)
    members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    _write_member(members[0], [1.0], preserve)
    _write_member(members[1], [2.0], preserve)
    member_stores = {"r1": members[0], "r2": members[1]}
    assert build_ensemble(member_stores, tmp_path, "tas", preserve)

    cleanup = _settings(settings, cleanup_members=True, ensemble_mode=EnsembleMode.STACK.value)
    assert build_ensemble(member_stores, tmp_path, "tas", cleanup)
    assert all(not path.exists() for path in members)


def test_cleanup_member_rename_failure_rolls_back_all_members(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    for member in members:
        member.mkdir()
    original_rename = Path.rename

    def fail_second_member(path: Path, target: Path) -> Path:
        if path == members[1] and target.name.startswith(".cleanup-"):
            raise OSError("simulated rename failure")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_second_member)
    with pytest.raises(OSError, match="simulated rename failure"):
        _cleanup_member_stores(members)

    assert all(member.exists() for member in members)
    assert not list(tmp_path.glob(".cleanup-*"))


def test_cleanup_member_removal_failure_leaves_warned_residual(
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    members = [tmp_path / "member-r1.zarr", tmp_path / "member-r2.zarr"]
    for member in members:
        member.mkdir()
    original_rmtree = shutil.rmtree
    failed = False

    def fail_first_cleanup(path: Path) -> None:
        nonlocal failed
        cleanup_path = Path(path)
        if cleanup_path.name.startswith(".cleanup-") and not failed:
            failed = True
            raise OSError("simulated removal failure")
        original_rmtree(cleanup_path)

    monkeypatch.setattr(shutil, "rmtree", fail_first_cleanup)
    with caplog.at_level(logging.WARNING, logger="src.writer"):
        _cleanup_member_stores(members)

    residuals = list(tmp_path.glob(".cleanup-*"))
    assert all(not member.exists() for member in members)
    assert len(residuals) == 1
    assert str(residuals[0]) in caplog.text
    assert "manual removal" in caplog.text
