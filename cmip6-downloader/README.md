# CMIP6 Downloader

This directory contains a Python workflow that resolves AWS and Google CMIP6 catalogs, downloads the selected assets, standardizes time and spatial coordinates, and writes local Zarr v2 member and ensemble stores.

The catalog union keeps assets available from either provider. AWS wins equivalent-asset ties by default, while Google remains available for exclusive assets and unambiguous fallback stores.

## Install

On Windows, use WSL2 with an Ubuntu or Debian distribution. Install Make and curl for your operating system:

Ubuntu, Debian, or WSL:

```bash
sudo apt update
sudo apt install -y make curl
```

Fedora:

```bash
sudo dnf install -y make curl
```

Arch Linux:

```bash
sudo pacman -S --needed make curl
```

macOS:

```bash
xcode-select --install
```

Install [uv](https://docs.astral.sh/uv/) and verify the tools:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
make --version
uv --version
```

From the repository root, create the environment and local configuration:

```bash
cd cmip6-downloader
make install
cp .env.example .env
```

Edit `.env`, then activate the environment:

```bash
source .venv/bin/activate
```

`make install` uses uv to install Python 3.12 and the locked runtime and development dependencies in `.venv`. Notebook support is optional:

```bash
uv sync --group notebook
```

## Configure

`.env` is the canonical operational configuration and `.env.example` contains every supported field. Every new execution reads `.env` again. Explicit script options temporarily override its values for that execution; omitted options preserve them.

All fields already have values in the copied `.env`, but they are required by `Settings`. Review these fields before the first run:

```dotenv
CATALOG_AWS_PATH=catalog/pangeo-cmip6_aws.csv
CATALOG_GOOGLE_PATH=catalog/pangeo-cmip6_google.csv
FILTERED_CATALOG_DIR=filtered_catalog
PREFERRED_CATALOG_PATH=filtered_catalog/catalog_preferred.csv
DOWNLOADS_DIR=downloads

PROVIDER_PRIORITY=["aws","google"]
AWS_ANONYMOUS=true
GOOGLE_ANONYMOUS=true

SOURCE_IDS=["MIROC6","CMCC-ESM2","ACCESS-CM2","BCC-CSM2-MR","INM-CM5-0","EC-Earth3-Veg"]
EXPERIMENT_IDS=["historical","ssp245","ssp585"]
TABLE_IDS=["day","3hr","Omon"]
VARIABLE_IDS=["tas","tasmax","huss","pr","ua","va","zg","wap","tos"]
GRID_LABELS=["gn","gr","gr1"]
MEMBER_IDS=[]

HISTORICAL_START=1950-01-01
HISTORICAL_END=2014-12-31
HISTORICAL_EXPERIMENTS=["historical"]
FUTURE_EXPERIMENTS=["ssp245","ssp585"]
FUTURE_START=2015-01-01
FUTURE_END=2050-12-31

LATITUDE_MIN=-70
LATITUDE_MAX=20
LONGITUDE_MIN=-120
LONGITUDE_MAX=-5
SPATIAL_SUBSET=true
EXCLUDED_VARIABLES=["tos"]
CURVILINEAR_POLICY=keep_global

MAX_WORKERS=4
TIME_CHUNK_SIZE=5760
OPEN_CHUNKS={}
EXISTING_POLICY=skip
ENSEMBLE_MODE=both
ENSEMBLE_ALIGNMENT=inner
CLEANUP_MEMBERS=true
MEMBER_STORE_TEMPLATE=member-{member_id}.zarr
```

Before the first run, review:

- Catalogs: place the AWS and Google CSV files at the configured paths and verify their schemas.
- Filters: confirm models, experiments, tables, variables, grids, and optional members.
- Periods: empty historical bounds keep all available historical dates; future experiments use the configured interval.
- Domain: confirm latitude and longitude bounds. Variables listed in `EXCLUDED_VARIABLES` remain global.
- Runtime: select worker and chunk sizes suitable for the machine.
- Policies: choose how existing stores, ensembles, and member cleanup should behave.
- Paths: verify catalog, filtered catalog, download, log, and store names. Relative paths resolve from `cmip6-downloader`.

`CLEANUP_MEMBERS=true` removes multiple member stores only after every requested ensemble has been written and validated. A sole member is always preserved. Outputs are Zarr v2 stores; this format is an application invariant rather than an `.env` option. See the **[complete setup and usage guide](docs/getting-started.md)** for every field, provider resolution, temporal coverage rules, and file policies.

## Run

Direct Python commands require the `.venv` activation shown above. Resolve the provider catalogs before starting the download:

```bash
python scripts/compare_catalogs.py
python scripts/run_download.py
```

The equivalent Make targets do not require virtual-environment activation:

```bash
make compare
make download
make all
```

`make all` runs catalog comparison followed by the download. The workflow writes the preferred catalog and decision reports under `filtered_catalog/`, then writes member stores, ensembles, group catalogs, and logs under `downloads/`.

See the **[complete setup and usage guide](docs/getting-started.md)** for configuration, provider fallback, outputs, Docker, and troubleshooting. See the **[Apptainer guide](docs/apptainer.md)** for HPC execution.
