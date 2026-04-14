# src/config.py

from pathlib import Path

# Define the root directory of the project
base_dir = Path(__file__).resolve().parents[2]

# Path to the directory containing CMIP6 catalog CSV files
catalog_dir = base_dir / "catalog"

# Path to the directory where downloaded datasets and logs will be saved
download_dir = base_dir / "downloads"

# Path to the AWS-based Pangeo CMIP6 catalog
catalog_aws = catalog_dir / "pangeo-cmip6_aws.csv"

# Path to the Google Cloud-based Pangeo CMIP6 catalog
catalog_google = catalog_dir / "pangeo-cmip6_google.csv"

# List of model names (source_id) to be included in the download
# Select your model to download in source_ids
source_ids = [
              "MIROC6",
              "CMCC-ESM2",
              "ACCESS-CM2",
              "BCC-CSM2-MR",
              "INM-CM5-0",
              "EC-Earth3-Veg",
              ]

# List of experiment types (experiment_id), e.g., historical and future scenarios
experiment_ids = [
                  "historical", 
                  "ssp245", 
                  "ssp585"
                  ]

# Table IDs indicating the time frequency of the data
table_ids = [
             "day", 
             "3hr", 
             "Omon"
             ]

# CMIP6 variable names to be downloaded and processed
variable_ids = [
                "tas",     # Near-surface air temperature
                "tasmax",  # Daily maximum near-surface air temperature
                "huss",    # Near-surface specific humidity
                "pr",      # Precipitation
                "ua",      # Zonal wind (pressure levels)
                "va",      # Meridional wind (pressure levels)
                "zg",      # Geopotential height (pressure levels)
                "wap",     # Omega: vertical velocity (pressure levels)
                "tos",     # Sea Surface Temperature
                ]

# Default chunk size for saving datasets in blocks along the time dimension
default_chunk_size = 5760  # ~16 years of daily or 8 months of 3-hourly data

# Standard calendar to which CFTimeIndex calendars will be converted
default_calendar = "proleptic_gregorian"

# Grouping

# Fields that define a group (excluding member_id/version)
group_fields = ("source_id", "experiment_id", "table_id", "variable_id", "grid_label")

# Pattern for individual member file names inside group folder
member_zarr_template = "member-{member_id}.zarr"

# Ensemble / post-processing
ensemble_mode = "both"      # create both: stacked ensemble and mean ("both", "stack", "mean")
time_align = "inner"        # 'inner' = intersection of time, 'outer' = union
ensemble_all_name = "ensemble_all.zarr"
ensemble_mean_name = "ensemble_mean.zarr"
cleanup_members = True      # delete members after ensemble created

# CSV outputs
group_catalog_filename = "catalog_group.csv"
group_log_filename = "group_log.csv"
global_log_filename = "log_download_results.csv"
