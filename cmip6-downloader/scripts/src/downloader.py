"""Provider-aware grouped CMIP6 download orchestration."""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

import gcsfs
import pandas as pd
import s3fs
import xarray as xr
from tqdm import tqdm

from src.config import Settings
from src.filters import (
    create_group_key,
    filter_catalog,
    group_relpath,
    normalize_fields,
    version_key,
)
from src.preprocessing import preprocess_dataset
from src.writer import build_ensemble, save_dataset_in_blocks, validate_store

LOGGER = logging.getLogger(__name__)

type StoreMapper = MutableMapping[str, bytes]
type LogRecord = dict[str, str]


def provider_mapper(url: str, provider: str, settings: Settings) -> StoreMapper:
    """Create a mapper with provider-specific anonymous options."""
    if provider == "aws":
        return s3fs.S3FileSystem(anon=settings.aws_anonymous).get_mapper(url)
    if provider == "google":
        token = "anon" if settings.google_anonymous else None
        return gcsfs.GCSFileSystem(token=token).get_mapper(url)
    raise ValueError(f"unsupported provider: {provider}")


def member_output_tasks(
    frame: pd.DataFrame,
    output_dir: Path,
    settings: Settings,
) -> dict[Path, pd.DataFrame]:
    """Create exactly one output task per group and member."""
    tasks: dict[Path, pd.DataFrame] = {}
    group_artifact_names = {
        settings.group_catalog_filename,
        settings.group_log_filename,
        settings.ensemble_all_filename,
        settings.ensemble_mean_filename,
    }
    for _, member_rows in frame.groupby(["group_key", "member_id"], sort=False):
        first = member_rows.iloc[0]
        member_id = str(first["member_id"])
        filename = settings.format_member_store(member_id)
        if filename in group_artifact_names:
            raise ValueError(f"member store filename collides with group artifact: {filename}")
        path = output_dir / group_relpath(first) / filename
        if not path.resolve().is_relative_to(output_dir.resolve()):
            raise ValueError(f"output path escapes downloads directory: {path}")
        if path in tasks:
            raise ValueError(f"duplicate output task: {path}")
        tasks[path] = member_rows.copy()
    return tasks


class CMIP6Downloader:
    """Download and preprocess assets from a preferred provider catalog."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.output_dir = settings.resolve_path(settings.downloads_dir)

    def load_and_filter_catalog(self) -> pd.DataFrame:
        """Load, validate, filter, and resolve rows before worker creation."""
        path = self.settings.resolve_path(self.settings.preferred_catalog_path)
        frame = filter_catalog(normalize_fields(pd.read_csv(path)), self.settings)
        if "provider" not in frame.columns:
            raise ValueError("preferred catalog is missing required column: provider")
        frame["group_key"] = create_group_key(frame)
        frame = frame.drop_duplicates().reset_index(drop=True)
        member_output_tasks(frame, self.output_dir, self.settings)
        return frame

    def _open_store(self, zstore: str, provider: str) -> xr.Dataset:
        mapper = provider_mapper(zstore, provider, self.settings)
        return xr.open_zarr(
            mapper,
            consolidated=self.settings.remote_consolidated,
            chunks=self.settings.open_chunks,
        )

    def _open_dataset(self, row: pd.Series) -> xr.Dataset:
        provider = str(row["provider"])
        zstore = str(row["zstore"])
        try:
            return self._open_store(zstore, provider)
        except (KeyError, OSError, RuntimeError, ValueError) as primary_error:
            alternate_provider = row.get("alternate_provider")
            alternate_zstore = row.get("alternate_zstore")
            if pd.isna(alternate_provider) or pd.isna(alternate_zstore):
                raise
            fallback_provider = str(alternate_provider).strip()
            fallback_zstore = str(alternate_zstore).strip()
            if not fallback_provider or not fallback_zstore:
                raise
            LOGGER.warning(
                "Opening %s from %s failed; falling back to %s at %s",
                zstore,
                provider,
                fallback_provider,
                fallback_zstore,
            )
            try:
                return self._open_store(fallback_zstore, fallback_provider)
            except (KeyError, OSError, RuntimeError, ValueError) as fallback_error:
                raise RuntimeError(
                    f"primary and alternate stores failed: {primary_error}; {fallback_error}"
                ) from fallback_error

    def process_member(self, rows: pd.DataFrame, output_path: Path) -> LogRecord:
        """Open all temporal segments and write one member store."""
        if not output_path.resolve().is_relative_to(self.output_dir.resolve()):
            raise ValueError(f"output path escapes downloads directory: {output_path}")
        datasets: list[xr.Dataset] = []
        processed: list[xr.Dataset] = []
        first = rows.iloc[0]
        variable_id = str(first["variable_id"])
        try:

            def segment_order(row: pd.Series) -> tuple[tuple[int, str], str]:
                return version_key(str(row["version"])), str(row["zstore"])

            ordered = sorted((row for _, row in rows.iterrows()), key=segment_order, reverse=True)
            for row in ordered:
                dataset = self._open_dataset(row)
                datasets.append(dataset)
                processed.append(
                    preprocess_dataset(
                        dataset,
                        self.settings,
                        str(row["experiment_id"]),
                        variable_id,
                    )
                )
            combined = processed[0]
            for older in processed[1:]:
                existing_times = pd.Index(combined.indexes["time"])
                older_times = pd.Index(older.indexes["time"])
                exclusive = older.isel(time=~older_times.isin(existing_times))
                if exclusive.sizes.get("time", 0):
                    combined = xr.concat([combined, exclusive], dim="time")
            combined = combined.sortby("time")
            written = save_dataset_in_blocks(combined, output_path, variable_id, self.settings)
            combined.close()
            return {
                "file": str(output_path),
                "status": "Success" if written else "Skipped",
                "message": "" if written else "Valid store already exists",
            }
        except (FileExistsError, KeyError, OSError, RuntimeError, ValueError) as error:
            LOGGER.error("Failed member %s: %s", output_path, error)
            return {"file": str(output_path), "status": "Error", "message": str(error)}
        finally:
            for dataset in datasets:
                dataset.close()

    def process_group(self, frame: pd.DataFrame) -> list[LogRecord]:
        """Download group members concurrently, then build its ensemble."""
        if frame.empty:
            return []
        first = frame.iloc[0]
        group_path = self.output_dir / group_relpath(first)
        if not group_path.resolve().is_relative_to(self.output_dir.resolve()):
            raise ValueError(f"group path escapes downloads directory: {group_path}")
        source_path = self.output_dir / str(first["source_id"])
        global_log_path = self.output_dir / self.settings.global_log_filename
        if source_path.resolve() == global_log_path.resolve():
            raise ValueError(f"group source directory collides with global log: {source_path}")
        group_path.mkdir(parents=True, exist_ok=True)
        frame.to_csv(group_path / self.settings.group_catalog_filename, index=False)
        tasks = member_output_tasks(frame, self.output_dir, self.settings)
        logs: list[LogRecord] = []
        if not tasks:
            pd.DataFrame(logs, columns=["file", "status", "message"]).to_csv(
                group_path / self.settings.group_log_filename,
                index=False,
            )
            return logs
        futures: dict[Future[LogRecord], Path] = {}
        with ThreadPoolExecutor(max_workers=min(self.settings.max_workers, len(tasks))) as executor:
            for path, rows in tasks.items():
                futures[executor.submit(self.process_member, rows, path)] = path
            for future in tqdm(
                as_completed(futures), total=len(futures), desc=str(first["group_key"])
            ):
                logs.append(future.result())
        if all(log["status"] in {"Success", "Skipped"} for log in logs):
            variable_id = str(first["variable_id"])
            try:
                for path in tasks:
                    validate_store(path, variable_id)
                member_paths = {
                    str(rows.iloc[0]["member_id"]): path for path, rows in tasks.items()
                }
                build_ensemble(member_paths, group_path, variable_id, self.settings)
            except (FileExistsError, KeyError, OSError, RuntimeError, ValueError) as error:
                LOGGER.error("Failed ensemble in %s: %s", group_path, error)
                logs.append(
                    {
                        "file": str(group_path),
                        "status": "Error",
                        "message": f"Ensemble failed: {error}",
                    }
                )
        pd.DataFrame(logs, columns=["file", "status", "message"]).to_csv(
            group_path / self.settings.group_log_filename,
            index=False,
        )
        return logs

    def run(self) -> pd.DataFrame:
        """Run all groups and write the global processing log."""
        catalog = self.load_and_filter_catalog()
        logs: list[LogRecord] = []
        for group_key, frame in catalog.groupby("group_key", sort=False):
            LOGGER.info("Processing group %s with %d catalog rows", group_key, len(frame))
            logs.extend(self.process_group(frame))
        result = pd.DataFrame(logs, columns=["file", "status", "message"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        result.to_csv(self.output_dir / self.settings.global_log_filename, index=False)
        return result
