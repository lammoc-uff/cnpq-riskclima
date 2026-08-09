"""Atomic local Zarr writing and ensemble creation."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import xarray as xr
import zarr

from src.config import ZARR_FORMAT_VERSION, EnsembleMode, ExistingPolicy, Settings

LOGGER = logging.getLogger(__name__)


def _time_coverage(dataset: xr.Dataset) -> tuple[int, str, str]:
    size = dataset.sizes.get("time", 0)
    if "time" not in dataset.coords or size == 0:
        raise ValueError("dataset has no non-empty time coordinate")
    index = dataset.indexes["time"]
    return size, str(index[0]), str(index[-1])


def validate_store(
    path: Path,
    variable_id: str,
    *,
    expected_time_size: int | None = None,
    expected_time_start: str | None = None,
    expected_time_end: str | None = None,
    expected_member_ids: list[str] | None = None,
    allow_partial: bool = False,
) -> None:
    """Validate store metadata eagerly without constructing Dask chunks."""
    if ".partial-" in path.name and not allow_partial:
        raise ValueError(f"partial store is never valid: {path}")
    try:
        with xr.open_zarr(path, consolidated=None, chunks=None) as dataset:
            if variable_id not in dataset.data_vars:
                raise ValueError(f"store {path} does not contain variable {variable_id}")
            time_size, time_start, time_end = _time_coverage(dataset)
            expected_coverage = (expected_time_size, expected_time_start, expected_time_end)
            actual_coverage = (time_size, time_start, time_end)
            for expected, actual, label in zip(
                expected_coverage,
                actual_coverage,
                ("time size", "time start", "time end"),
                strict=True,
            ):
                if expected is not None and expected != actual:
                    raise ValueError(f"store {path} has unexpected {label}: {actual!r}")
            if expected_member_ids is not None:
                actual_ids = list(dataset.attrs.get("source_member_ids", []))
                actual_count = dataset.attrs.get("source_member_count")
                if actual_ids != expected_member_ids or actual_count != len(expected_member_ids):
                    raise ValueError(f"store {path} has stale member provenance")
                attrs_coverage = (
                    dataset.attrs.get("source_time_count"),
                    dataset.attrs.get("source_time_start"),
                    dataset.attrs.get("source_time_end"),
                )
                if attrs_coverage != actual_coverage:
                    raise ValueError(f"store {path} has stale coverage provenance")
    except (KeyError, OSError, TypeError, ValueError, zarr.errors.PathNotFoundError) as error:
        raise ValueError(f"existing store is invalid: {path}: {error}") from error


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


@contextmanager
def _output_lock(path: Path) -> Generator[None, None, None]:
    lock_path = path.with_name(f".{path.name}.lock")
    try:
        lock_path.mkdir()
    except FileExistsError as error:
        raise FileExistsError(f"output store is locked: {path}") from error
    try:
        yield
    finally:
        _remove_path(lock_path)


def _check_existing(
    path: Path,
    variable_id: str,
    policy: ExistingPolicy,
    expected_time_size: int,
    expected_time_start: str,
    expected_time_end: str,
    expected_member_ids: list[str] | None = None,
) -> bool:
    if not path.exists():
        return True
    if policy is ExistingPolicy.FAIL:
        raise FileExistsError(f"output store already exists: {path}")
    if policy is ExistingPolicy.SKIP:
        validate_store(
            path,
            variable_id,
            expected_time_size=expected_time_size,
            expected_time_start=expected_time_start,
            expected_time_end=expected_time_end,
            expected_member_ids=expected_member_ids,
        )
        LOGGER.info("Skipping valid existing store %s", path)
        return False
    return True


def _promote_store(temporary: Path, destination: Path, backup: Path) -> None:
    had_destination = destination.exists()
    if had_destination:
        destination.rename(backup)
    try:
        temporary.rename(destination)
    except OSError:
        if had_destination and backup.exists() and not destination.exists():
            backup.rename(destination)
        raise
    _remove_path(backup)


def _restore_or_remove_backup(backup: Path, destination: Path) -> None:
    if not backup.exists():
        return
    if destination.exists():
        _remove_path(backup)
    else:
        backup.rename(destination)


def _cleanup_member_stores(member_paths: list[Path]) -> None:
    renamed: list[tuple[Path, Path]] = []
    try:
        for member_path in member_paths:
            temporary = member_path.with_name(f".cleanup-{uuid4().hex}")
            member_path.rename(temporary)
            renamed.append((member_path, temporary))
    except OSError as rename_error:
        rollback_errors: list[OSError] = []
        for member_path, temporary in reversed(renamed):
            try:
                temporary.rename(member_path)
            except OSError as rollback_error:
                rollback_errors.append(rollback_error)
        if rollback_errors:
            raise ExceptionGroup(
                "member cleanup rename failed and rollback was incomplete",
                [rename_error, *rollback_errors],
            ) from None
        raise

    residual_paths: list[Path] = []
    for _, temporary in renamed:
        try:
            _remove_path(temporary)
        except OSError:
            if temporary.exists():
                residual_paths.append(temporary)
    if residual_paths:
        LOGGER.warning(
            "Member cleanup left temporary paths for manual removal: %s",
            ", ".join(str(path) for path in residual_paths),
        )


def save_dataset_in_blocks(
    ds: xr.Dataset,
    output_path: Path,
    variable_id: str,
    settings: Settings,
) -> bool:
    """Write, validate, and atomically promote a time-chunked Zarr v2 store."""
    expected_size, expected_start, expected_end = _time_coverage(ds)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f"{output_path.name}.partial-{uuid4().hex}")
    backup = output_path.with_name(f"{output_path.name}.backup-{uuid4().hex}")
    with _output_lock(output_path):
        try:
            if not _check_existing(
                output_path,
                variable_id,
                settings.existing_policy,
                expected_size,
                expected_start,
                expected_end,
            ):
                return False
            for offset in range(0, expected_size, settings.time_chunk_size):
                block = ds.isel(time=slice(offset, offset + settings.time_chunk_size)).load()
                try:
                    block.to_zarr(
                        temporary,
                        mode="w" if offset == 0 else "a",
                        append_dim=None if offset == 0 else "time",
                        consolidated=False,
                        zarr_format=ZARR_FORMAT_VERSION,
                    )
                finally:
                    block.close()
            if settings.output_consolidated:
                zarr.consolidate_metadata(zarr.DirectoryStore(str(temporary)))
            validate_store(
                temporary,
                variable_id,
                expected_time_size=expected_size,
                expected_time_start=expected_start,
                expected_time_end=expected_end,
                allow_partial=True,
            )
            _promote_store(temporary, output_path, backup)
        finally:
            _remove_path(temporary)
            _restore_or_remove_backup(backup, output_path)
    LOGGER.info("Wrote member store %s", output_path)
    return True


def _ensemble_chunks(dataset: xr.Dataset, settings: Settings) -> dict[str, int]:
    chunks: dict[str, int] = {}
    for raw_dimension, size in dataset.sizes.items():
        dimension = str(raw_dimension)
        if dimension == "time":
            chunks[dimension] = min(size, settings.time_chunk_size)
        else:
            chunk_size = settings.ensemble_dimension_chunks.get(
                dimension, settings.ensemble_default_chunk_size
            )
            chunks[dimension] = min(size, chunk_size)
    return chunks


def _write_ensemble(
    dataset: xr.Dataset,
    path: Path,
    variable_id: str,
    member_ids: list[str],
    settings: Settings,
) -> bool:
    time_size, time_start, time_end = _time_coverage(dataset)
    dataset.attrs.update(
        {
            "source_member_ids": member_ids,
            "source_member_count": len(member_ids),
            "source_time_count": time_size,
            "source_time_start": time_start,
            "source_time_end": time_end,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.partial-{uuid4().hex}")
    backup = path.with_name(f"{path.name}.backup-{uuid4().hex}")
    with _output_lock(path):
        try:
            if not _check_existing(
                path,
                variable_id,
                settings.existing_policy,
                time_size,
                time_start,
                time_end,
                member_ids,
            ):
                return True
            dataset.chunk(_ensemble_chunks(dataset, settings)).to_zarr(
                temporary,
                mode="w",
                consolidated=settings.output_consolidated,
                zarr_format=ZARR_FORMAT_VERSION,
            )
            validate_store(
                temporary,
                variable_id,
                expected_time_size=time_size,
                expected_time_start=time_start,
                expected_time_end=time_end,
                expected_member_ids=member_ids,
                allow_partial=True,
            )
            _promote_store(temporary, path, backup)
        finally:
            _remove_path(temporary)
            _restore_or_remove_backup(backup, path)
    return True


def build_ensemble(
    member_stores: dict[str, Path],
    output_dir: Path,
    variable_id: str,
    settings: Settings,
) -> bool:
    """Build configured ensemble products and clean multiple members on success."""
    ordered_members = sorted(member_stores.items())
    member_ids = [member_id for member_id, _ in ordered_members]
    member_paths = [path for _, path in ordered_members]
    if len(set(member_paths)) != len(member_paths):
        raise ValueError("ensemble member IDs must map to distinct stores")
    if len(member_paths) <= 1 or settings.ensemble_mode is EnsembleMode.NONE:
        return False
    for path in member_paths:
        validate_store(path, variable_id)
    datasets: list[xr.Dataset] = []
    success = False
    try:
        for path in member_paths:
            datasets.append(
                xr.open_zarr(
                    path,
                    consolidated=settings.output_consolidated,
                    chunks=settings.open_chunks,
                )
            )
        aligned = xr.align(*datasets, join=settings.ensemble_alignment.value)
        stacked = xr.concat(aligned, dim="member", combine_attrs="override")
        stacked = stacked.assign_coords(member=("member", member_ids))
        bounds = [name for name in stacked.variables if str(name).endswith(("_bnds", "_bounds"))]
        stacked = stacked.drop_vars(bounds, errors="ignore")
        if "bnds" in stacked.dims:
            stacked = stacked.drop_dims("bnds", errors="ignore")
        for name in stacked.variables:
            stacked[name].encoding.pop("chunks", None)

        success = True
        if settings.ensemble_mode in {EnsembleMode.STACK, EnsembleMode.BOTH}:
            success &= _write_ensemble(
                stacked,
                output_dir / settings.ensemble_all_filename,
                variable_id,
                member_ids,
                settings,
            )
        if settings.ensemble_mode in {EnsembleMode.MEAN, EnsembleMode.BOTH}:
            mean = stacked.mean(dim="member", keep_attrs=True)
            success &= _write_ensemble(
                mean,
                output_dir / settings.ensemble_mean_filename,
                variable_id,
                member_ids,
                settings,
            )
            mean.close()
        stacked.close()
    finally:
        for dataset in datasets:
            dataset.close()
    if success and settings.cleanup_members:
        _cleanup_member_stores(member_paths)
    return success
