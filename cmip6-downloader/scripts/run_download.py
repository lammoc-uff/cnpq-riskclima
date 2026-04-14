# scripts/run_download.py

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import os

import pandas as pd
from tqdm.auto import tqdm # type: ignore

from src.downloader import CMIP6Downloader
from src.config import catalog_aws, download_dir, group_catalog_filename, group_log_filename, global_log_filename
from src.filters import group_relpath
from src.writer import build_ensemble
from src.config import member_zarr_template


def main():
    """
    Run the CMIP6 download pipeline group by group using parallel threads.
    """
    downloader = CMIP6Downloader(catalog_path=catalog_aws)

    # Load and filter catalog
    catalog_df = downloader.load_and_filter_catalog()
    
    ###catalog_df = catalog_df.tail(1).copy()  # Testing download of one member of one group
    ###Line to test an especific group
    ###catalog_df = catalog_df[
    ###                       (catalog_df["experiment_id"] == "historical") &
    ###                       (catalog_df["variable_id"] == "tos")
    ###                       ].copy()
    ###print(catalog_df)
    
    # Select number of workers
    cpu_count = os.cpu_count() or 1
    max_workers = max(1, cpu_count - 2)

    all_logs = []

    # Iterate group by group
    for gkey, df_group in catalog_df.groupby("group_key"):
        print(f"\n=== Processing group {gkey} ({len(df_group)} members) ===")

        first_row = df_group.iloc[0]
        group_path = download_dir / group_relpath(first_row)
        group_path.mkdir(parents=True, exist_ok=True)

        # Save catalog of this group
        df_group.to_csv(group_path / group_catalog_filename, index=False)

        # Process members in parallel
        group_log = []
        with ThreadPoolExecutor(max_workers=min(max_workers, len(df_group))) as executor:
            futures = {
                executor.submit(downloader.process_row, row, group_path): idx
                for idx, row in df_group.iterrows()
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Group {gkey}"):
                result = future.result()
                group_log.append(result)

        # Save group log
        pd.DataFrame(group_log).to_csv(group_path / group_log_filename, index=False)

        # Build ensemble
        member_paths = [
            group_path / member_zarr_template.format(member_id=row["member_id"])
            for _, row in df_group.iterrows()
            if (group_path / member_zarr_template.format(member_id=row["member_id"])).exists()
        ]
        
        # Calculate ensemble members for more than one member in member_paths
        if len(member_paths) > 1:
            build_ensemble(member_paths, group_path)

        all_logs.extend(group_log)

    # Save global log
    log_df = pd.DataFrame(all_logs)
    log_df.to_csv(download_dir / global_log_filename, index=False)
    print(f"\n📄 Download log saved to: {download_dir / global_log_filename}")


if __name__ == "__main__":
    main()
