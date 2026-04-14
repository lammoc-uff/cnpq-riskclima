# src/downloader.py

from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
import xarray as xr
import fsspec

from src.config import (
                        default_chunk_size,
                        default_calendar,
                        download_dir,
                        member_zarr_template,
                        ensemble_mode,
                        group_catalog_filename,
                        group_log_filename,
                        global_log_filename,
                        )

from src.filters import (
                         normalize_fields,
                         create_unique_key,
                         create_group_key,
                         group_relpath,
                         filter_catalog,
                        )

from src.preprocessing import preprocess_time, adjust_coordinates_flex, crop_to_brazil
from src.writer import save_dataset_in_blocks, build_ensemble


class CMIP6Downloader:
    """
    Orchestrates the download, filtering, and block-wise saving of CMIP6 datasets,
    grouped by (source, experiment, table, variable, grid).
    """

    def __init__(
        self,
        catalog_path: Path,
        output_dir: Optional[Path] = None,
        chunk_size: int = default_chunk_size,
        calendar: str = default_calendar,
    ):
        self.catalog_path = catalog_path
        self.output_dir = output_dir or download_dir
        self.chunk_size = chunk_size
        self.calendar = calendar

        self.catalog = None  # Loaded and filtered catalog
        self.log: List[Dict] = []  # Global log

    # --- Catalog handling ----------------------------------------------------

    def load_and_filter_catalog(self) -> pd.DataFrame:
        """Load, normalize, filter, and enrich catalog with keys."""
        df = pd.read_csv(self.catalog_path)
        df = normalize_fields(df)
        df = filter_catalog(df)

        df["key"] = create_unique_key(df)
        df["group_key"] = create_group_key(df)

        df = df.drop_duplicates(subset="key").copy()
        self.catalog = df
        return df

    # --- Dataset opening -----------------------------------------------------

    def _open_dataset(self, row: pd.Series) -> Optional[xr.Dataset]:
        """Open a Zarr dataset remotely via fsspec."""
        zarr_url = row["zstore"]
        try:
            mapper = fsspec.get_mapper(zarr_url, anon=True)
            ds = xr.open_zarr(mapper, consolidated=True, chunks={})
            return ds
        except Exception as e:
            print(f"❌ Failed to open {zarr_url} -> {e}")
            return None

    # --- Processing of a single member --------------------------------------

    def process_row(self, row: pd.Series, group_path: Path) -> Dict:
        """Download, preprocess, and save a single member."""
        ensemble_all_path = group_path / "ensemble_all.zarr"
        ensemble_mean_path = group_path / "ensemble_mean.zarr"
        
        # Checking if there is the ensembles
        if ensemble_mode == "both":
            if ensemble_all_path.exists() and ensemble_mean_path.exists():
                return {
                        "file": f"{ensemble_all_path.name}, {ensemble_mean_path.name}",
                        "status": "Skipped",
                        "message": "Ensemble 'both' already exists"
                        }
        if ensemble_mode == "stack":
            if ensemble_all_path.exists():
                return {
                        "file": str(ensemble_all_path.name),
                        "status": "Skipped",
                        "message": "Ensemble 'stack' already exists"
                        }
        if ensemble_mode == "mean":
            if ensemble_mean_path.exists():
                return {
                        "file": str(ensemble_mean_path.name),
                        "status": "Skipped",
                        "message": "Ensemble 'mean' already exists"
                        }
        
        # Processing each members individually
        member_path = group_path / member_zarr_template.format(member_id=row["member_id"])

        if member_path.exists():
            return {"file": str(member_path), "status": "Skipped", "message": "Already exists"}

        try:
            ds = self._open_dataset(row)
            if ds is None:
                return {"file": str(member_path), "status": "Error", "message": "Failed to open"}

            if "time" not in ds.dims:
                return {"file": str(member_path), "status": "No time", "message": "Missing 'time'"}

            # Preprocess
            ds = preprocess_time(ds, calendar=self.calendar)
            ds = adjust_coordinates_flex(ds)
            ds = crop_to_brazil(ds)

            # Normalizing before saving
            ds = ds.chunk({"time": self.chunk_size})

            # Save in blocks
            member_path.parent.mkdir(parents=True, exist_ok=True)
            save_dataset_in_blocks(ds, member_path, chunk_size=self.chunk_size)

            return {"file": str(member_path), "status": "Success", "message": ""}

        except Exception as e:
            return {"file": str(member_path), "status": "Error", "message": str(e)}

    # --- Processing of a group ----------------------------------------------

    def process_group(self, df_group: pd.DataFrame) -> pd.DataFrame:
        """Process all members of one group, then build ensemble."""
        first_row = df_group.iloc[0]
        group_path = self.output_dir / group_relpath(first_row)

        # Save group catalog
        group_path.mkdir(parents=True, exist_ok=True)
        df_group.to_csv(group_path / group_catalog_filename, index=False)

        # Process members sequentially (paralelismo fica no run_download)
        group_log = []
        for _, row in df_group.iterrows():
            result = self.process_row(row, group_path)
            group_log.append(result)

        # Save group log
        pd.DataFrame(group_log).to_csv(group_path / group_log_filename, index=False)

        # Build ensemble if members succeeded
        member_paths = [
            group_path / member_zarr_template.format(member_id=row["member_id"])
            for _, row in df_group.iterrows()
            if (group_path / member_zarr_template.format(member_id=row["member_id"])).exists()
        ]
        build_ensemble(member_paths, group_path)

        return pd.DataFrame(group_log)

    # --- Run all groups ------------------------------------------------------

    def run_grouped(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Run processing group by group."""
        if self.catalog is None:
            self.load_and_filter_catalog()

        df_to_process = self.catalog if limit is None else self.catalog.tail(limit)

        all_logs = []
        for gkey, df_group in df_to_process.groupby("group_key"):
            print(f"\n=== Processing group {gkey} ({len(df_group)} members) ===")
            group_log = self.process_group(df_group)
            all_logs.append(group_log)

        # Save global log
        global_log = pd.concat(all_logs, ignore_index=True)
        global_log.to_csv(self.output_dir / global_log_filename, index=False)
        self.log = global_log.to_dict(orient="records")

        return global_log