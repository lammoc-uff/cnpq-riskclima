# src/writer.py

import warnings
import gc
from pathlib import Path
from typing import List

import xarray as xr
from zarr.errors import ZarrUserWarning

from src.config import (
    member_zarr_template,
    default_chunk_size,
    ensemble_all_name,
    ensemble_mean_name,
    ensemble_mode,
    time_align,
    cleanup_members,
)


def save_dataset_in_blocks(
    ds: xr.Dataset,
    output_path: Path,
    chunk_size: int = 5760,
) -> None:
    """
    Save an xarray.Dataset to Zarr format in time-sliced blocks.

    This prevents memory overload by writing small slices
    along the 'time' dimension incrementally.
    """
    print(ds.dims)

    if "time" not in ds.dims:
        print(f"⚠️ Skipping {output_path.name}: no 'time' dimension.")
        return

    time_len = ds.sizes["time"]

    for i in range(0, time_len, chunk_size):
        block = ds.isel(time=slice(i, i + chunk_size)).load()

        ds_clean = xr.Dataset(
            data_vars={
                var: (block[var].dims, block[var].values)
                for var in block.data_vars
            },
            coords={
                coord: (block[coord].dims, block[coord].values)
                for coord in block.coords
            },
            attrs=block.attrs,
        )

        mode = "w" if i == 0 else "a"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ZarrUserWarning)
            if mode == "w":
                ds_clean.to_zarr(output_path, mode=mode)
            else:
                ds_clean.to_zarr(output_path, mode=mode, append_dim="time")

        print(f"✔️ Saved block {i}–{min(i + chunk_size, time_len)} to {output_path}")

        del ds_clean, block
        gc.collect()


def build_ensemble(
    member_paths: List[Path],
    output_dir: Path,
) -> None:
    """
    Build ensemble datasets from a list of per-member Zarr stores.

    Creates:
      - ensemble_all.zarr : all members stacked along 'member'
      - ensemble_mean.zarr: mean across members

    Parameters
    ----------
    member_paths : list of Path
        Paths to per-member Zarr stores.
    output_dir : Path
        Folder where ensemble outputs will be written.
    """
    if not member_paths:
        print(f"⚠️ No members found in {output_dir}, skipping ensemble.")
        return

    datasets = []
    member_ids = []

    # Open each member (local zarr stores, not consolidated)
    for p in member_paths:
        try:
            ds = xr.open_zarr(p, consolidated=False)
            print(ds.dims)
            datasets.append(ds)
            member_ids.append(p.stem.replace("member-", "").replace(".zarr", ""))
        except Exception as e:
            print(f"❌ Failed to open {p}: {e}")

    if not datasets:
        print(f"⚠️ No valid members in {output_dir}, ensemble skipped.")
        return

    # Align and stack along new dimension 'member'
    try:
        aligned = xr.align(*datasets, join=time_align)
        stacked = xr.concat(aligned, dim="member")
        stacked = stacked.assign_coords(member=("member", member_ids))
        bounds_names = [v for v in stacked.variables if v.endswith("_bnds") or v.endswith("_bounds")]
        stacked = stacked.drop_vars(bounds_names, errors="ignore")

        if "bnds" in stacked.dims:
            stacked = stacked.drop_dims("bnds", errors="ignore")
        for v in stacked.variables:
            stacked[v].encoding.pop("chunks", None)
        
        chunk_map = {}

        for dim, n in stacked.sizes.items():
            if dim == "member":
                chunk_map[dim] = 1
            elif dim == "time":
                chunk_map[dim] = default_chunk_size
            elif dim in ("lev", "plev"):
                chunk_map[dim] = 1
            elif n <= 16:
                chunk_map[dim] = n
            else:
                chunk_map[dim] = min(n, 64)

        stacked = stacked.chunk(chunk_map)
        
    except Exception as e:
        print(f"❌ Ensemble alignment/stacking failed in {output_dir}: {e}")
        return

    # Save ensemble_all
    if ensemble_mode in ("stack", "both"):
        stacked.to_zarr(output_dir / ensemble_all_name, mode="w")
        print(f"✔️ Saved stacked ensemble → {ensemble_all_name}")

    # Save ensemble_mean
    if ensemble_mode in ("mean", "both"):
        stacked.mean(dim="member").to_zarr(output_dir / ensemble_mean_name, mode="w")
        print(f"✔️ Saved mean ensemble → {ensemble_mean_name}")

    # Cleanup
    if cleanup_members:
        for p in member_paths:
            try:
                if p.is_dir():
                    import shutil
                    shutil.rmtree(p)
            except Exception as e:
                print(f"⚠️ Failed to remove {p}: {e}")
        print("🧹 Members cleaned up after ensemble.")