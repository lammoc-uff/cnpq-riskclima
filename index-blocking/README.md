# RiskClima atmospheric blocking index

This directory contains workflows to generate monthly reference climatologies and calculate daily atmospheric blocking series from ERA5 and CMIP6 data. The scientific basis for the index is described in [Creation and Assessment of an Index for Atmospheric Blockings in Brazil’s Central Region](https://irispublishers.com/ahm/fulltext/creation-and-assessment-of-an-index-for-atmospheric.ID.000519.php), by Cataldi et al. (2024), DOI [10.33552/AHM.2024.01.000519](https://doi.org/10.33552/AHM.2024.01.000519).

The blocking condition requires positive relative vorticity at 850 and 500 hPa and a positive 500 hPa geopotential anomaly. The conditions must persist for three consecutive days. Once the threshold is reached, the preceding days in that event are classified as blocking.

## Install

On Windows, use WSL2 with an Ubuntu or Debian distribution. Install Make and curl:

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

From the repository root:

```bash
cd index-blocking
make install
cp .env.example .env
```

The direct uv equivalent is:

```bash
uv sync --frozen
```

## Configure

`.env` is the operational configuration. Relative paths resolve from the current working directory, so run commands from `index-blocking`.

The default configuration uses:

```dotenv
BLOCKING_BASE_DIRECTORY=.
BLOCKING_PERSISTENCE_DAYS=3

ERA5_CLIMATOLOGY_START_YEAR=1991
ERA5_CLIMATOLOGY_END_YEAR=2020
ERA5_CLIMATOLOGY_LABEL=90_20
ERA5_SERIES_START_YEAR=1960
ERA5_SERIES_END_YEAR=2025
ERA5_RAW_DIRECTORY=era5/raw_data
ERA5_PROCESSED_DIRECTORY=era5/processed_data
ERA5_RESULTS_DIRECTORY=era5/results

CMIP6_DATA_DIRECTORY=cmip6/raw_data
CMIP6_PROCESSED_DIRECTORY=cmip6/processed_data
CMIP6_RESULTS_DIRECTORY=cmip6/results
CMIP6_SOURCE_ID=BCC-CSM2-MR
CMIP6_CLIMATOLOGY_EXPERIMENT_ID=historical
CMIP6_EXPERIMENT_ID=ssp585
CMIP6_CLIMATOLOGY_START_YEAR=1980
CMIP6_CLIMATOLOGY_END_YEAR=2010
CMIP6_SERIES_START_YEAR=2015
CMIP6_SERIES_END_YEAR=2050
CMIP6_CLIMATOLOGY_LABEL=80_10

CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=
CDSAPI_CONFIG_FILE=~/.cdsapirc
```

`CDSAPI_KEY` takes precedence over `CDSAPI_CONFIG_FILE`. The key is never committed.

## Workflow

The four stages are independent Make targets. The climatology must exist before its corresponding series is calculated, but Make does not chain the stages automatically.

```bash
make climatology-era5
make series-era5

make climatology-cmip6
make series-cmip6
```

Direct entry points are also available:

```bash
uv run --frozen riskclima-blocking-climatology-era5
uv run --frozen riskclima-blocking-series-era5
uv run --frozen riskclima-blocking-climatology-cmip6
uv run --frozen riskclima-blocking-series-cmip6
```

## Requirements

ERA5 requires a valid CDS API configuration, network access to CDS, and the `cdo` executable for monthly climatology generation. CMIP6 requires local pressure-level Zarr stores containing `ua`, `va`, and `zg`.

The workflows do not perform preflight checks or scientific fallbacks for missing external tools or data. Their native errors report unavailable requirements.

## Data layout

```text
era5/
├── raw_data/
├── processed_data/
└── results/

cmip6/
├── raw_data/
├── processed_data/
└── results/
```

CMIP6 stores are expected below `CMIP6_DATA_DIRECTORY`:

```text
{SOURCE_ID}/{EXPERIMENT_ID}/day/{variable_id}/{grid_label}/*.zarr
```

## Outputs

ERA5 climatology and results are written below:

```text
era5/processed_data/clima_gz_90_20.nc
```

CMIP6 climatology and results are written below:

```text
cmip6/processed_data/{SOURCE_ID}_clima_zg500_80_10.nc
cmip6/results/{SOURCE_ID}/{EXPERIMENT_ID}/
```

Each series output contains regional predictor files and `daily_blocking_series.csv`. The predefined regions are `total`, `north`, `north_h1`, `north_h2`, `south`, `south_h1`, and `south_h2`.

## Docker

Build the runtime image from `index-blocking`:

```bash
docker build --target runtime -t riskclima-blocking:local .
```

Mount the project to keep `.env`, data, and outputs outside the image:

```bash
docker run --rm \
  --env-file .env \
  --volume "$PWD:/work" \
  --workdir /work \
  riskclima-blocking:local \
  riskclima-blocking-series-era5
```

The image includes Python dependencies and CDO. It does not include climate data or credentials.

## Apptainer

Convert the Docker runtime image to SIF:

```bash
docker build --target runtime -t riskclima-blocking:local .
apptainer build riskclima-blocking.sif docker-daemon://riskclima-blocking:local
```

Run an entry point with the project bound at `/work`:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-blocking.sif \
  riskclima-blocking-series-era5
```

See [`docs/apptainer.md`](docs/apptainer.md) for the operational commands.

## Documentation

See [`docs/blocking-index.md`](docs/blocking-index.md) for the scientific workflow, variable definitions, geographic regions, and ERA5/CMIP6 differences.

## Scientific reference

Cataldi M, Ribeiro EM, Andrade LS, Lima AP, Almeida GLM, Pereira TRAP (2024). Creation and Assessment of an Index for Atmospheric Blockings in Brazil’s Central Region. *Advances in Hydrology & Meteorology*, 1(4). [Article](https://irispublishers.com/ahm/fulltext/creation-and-assessment-of-an-index-for-atmospheric.ID.000519.php) · [DOI](https://doi.org/10.33552/AHM.2024.01.000519).
