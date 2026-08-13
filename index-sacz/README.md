# RiskClima SACZ index

This directory contains Python workflows for the South Atlantic Convergence Zone (SACZ) index using ERA5 and CMIP6 atmospheric data. The scientific context for the SACZ predictors is described in [Dynamics-based regression models for the South Atlantic Convergence Zone](https://link.springer.com/article/10.1007/s00382-018-4460-4), by Nielsen et al. (2019), DOI [10.1007/s00382-018-4460-4](https://doi.org/10.1007/s00382-018-4460-4). Each source has two independent stages:

1. preprocessing atmospheric fields into the CSV predictors required by the statistical model;
2. calculating the SACZ index for the configured year or CMIP6 period.

The repository contains the workflow code, internal scientific catalogs and utilities, and configuration templates. The SACZ area shapefile and model coefficient files are required runtime assets and must be provided separately.

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

From the repository root, enter `index-sacz` and install the Python environment:

```bash
cd index-sacz
uv sync --frozen
```

The equivalent Make target is:

```bash
make install
```

Copy the example configuration before running a workflow:

```bash
cp .env.example .env
```

Relative paths in `.env` resolve from the current working directory. Run commands from `index-sacz`.

## Configuration

`.env.example` contains the shared paths, ERA5 year, and CMIP6 model configuration:

```dotenv
# Shared SACZ paths
SACZ_BASE_DIRECTORY=.
SACZ_AREAS_FILE=areas/sams_index_calc_areas.shp
SACZ_COEFFICIENTS_DIRECTORY=coefs

# ERA5
ERA5_YEAR=2020
ERA5_RAW_DIRECTORY=era5/raw_data
ERA5_INPUT_DIRECTORY=era5/processed_data
ERA5_INTERMEDIATES_DIRECTORY=era5/processed_data/intermediates
ERA5_OUTPUT_DIRECTORY=era5/results

# CMIP6
CMIP6_DATA_DIRECTORY=cmip6/raw_data
CMIP6_INPUT_DIRECTORY=cmip6/processed_data
CMIP6_INTERMEDIATES_DIRECTORY=cmip6/processed_data/intermediates
CMIP6_OUTPUT_DIRECTORY=cmip6/results
CMIP6_SOURCE_ID=BCC-CSM2-MR
CMIP6_EXPERIMENT_ID=ssp245
CMIP6_START_YEAR=2015
CMIP6_END_YEAR=2050
```

Optional command-line arguments override values loaded from `.env`. For example:

```bash
make process-era5 ARGS="--era5-year 2021"
make process-cmip6 ARGS="--cmip6-source-id BCC-CSM2-MR --cmip6-experiment-id ssp245"
```

## Workflow

The Makefile exposes the four stages independently. It does not combine or chain them:

```bash
make process-era5
make index-era5

make process-cmip6
make index-cmip6
```

The equivalent direct commands are:

```bash
uv run --frozen riskclima-sacz-process-era5
uv run --frozen riskclima-sacz-index-era5
uv run --frozen riskclima-sacz-process-cmip6
uv run --frozen riskclima-sacz-index-cmip6
```

Run preprocessing before the corresponding index calculation. Existing intermediate files may be reused by the scripts according to their current file-existence checks.

## Required runtime assets

The following assets are not included in the repository yet:

### SACZ areas

Place the complete shapefile bundle at the path configured by `SACZ_AREAS_FILE`. The expected default files are:

```text
areas/sams_index_calc_areas.shp
areas/sams_index_calc_areas.shx
areas/sams_index_calc_areas.dbf
areas/sams_index_calc_areas.prj
areas/sams_index_calc_areas.cpg
```

The files must remain together. See [`areas/README.md`](areas/README.md).

### Model coefficients

Place the coefficient files under:

```text
coefs/
├── step1/
│   ├── scale_coefs_AB.csv
│   ├── scale_coefs_C.csv
│   └── scale_coefs_DE.csv
├── step2/
│   ├── pc_weights_AB.csv
│   ├── pc_weights_C.csv
│   └── pc_weights_DE.csv
└── step3/
    ├── betas_AB.csv
    ├── betas_C.csv
    └── betas_DE.csv
```

The coefficient files are shared by ERA5 and CMIP6. Their schemas must match the columns and indexes read by the calculation scripts. See [`coefs/README.md`](coefs/README.md).

## ERA5 requirements

The ERA5 preprocessing downloads pressure-level fields from the public ARCO ERA5 Google Cloud bucket. It additionally uses:

- [CDO](https://code.mpimet.mpg.de/projects/cdo) for daily means and NetCDF merging;
- `gsutil` from the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) for downloads.

The workflow does not configure credentials for the public bucket. Install and configure these tools according to their official documentation.

ERA5 outputs use the configured directories:

```text
era5/raw_data/{YEAR}/
era5/processed_data/{YEAR}/
```

## CMIP6 requirements

CMIP6 atmospheric fields must already be available locally as Zarr stores. The expected structure below the configured `CMIP6_DATA_DIRECTORY` is:

```text
{SOURCE_ID}/{EXPERIMENT_ID}/day/{variable_id}/{grid_label}/*.zarr
```

The preprocessing reads `ua`, `va`, `wap`, and `zg`, selects the required pressure levels, computes divergence and vorticity, applies the SACZ area polygons, and writes predictor CSV files under `CMIP6_INPUT_DIRECTORY`.

Zarr v3 data require a compatible Python, Xarray, and Zarr installation. The project uses Python 3.12 and declares `zarr>=3`.

## Outputs

The index stages write three regional CSV files:

```text
era5/results/{YEAR}/AB.csv
era5/results/{YEAR}/C.csv
era5/results/{YEAR}/DE.csv

cmip6/results/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/AB.csv
cmip6/results/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/C.csv
cmip6/results/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/DE.csv
```

Intermediate statistical-model files are written under the configured `*_INTERMEDIATES_DIRECTORY`.

## Docker

Build the runtime image from `index-sacz`:

```bash
docker build --target runtime -t riskclima-sacz:local .
```

Mount the project so configuration, areas, coefficients, climate data, and results remain outside the image:

```bash
docker run --rm \
  --env-file .env \
  --volume "$PWD:/work" \
  --workdir /work \
  riskclima-sacz:local \
  riskclima-sacz-process-era5
```

Run CMIP6 by changing the command to `riskclima-sacz-process-cmip6` or `riskclima-sacz-index-cmip6`.

## Apptainer

The Docker runtime image can be converted to SIF:

```bash
docker build --target runtime -t riskclima-sacz:local .
apptainer build riskclima-sacz.sif docker-daemon://riskclima-sacz:local
```

Bind the project directory when running an entry point:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-sacz.sif \
  riskclima-sacz-process-era5
```

See [`docs/apptainer.md`](docs/apptainer.md) for the binding requirements.

## Current limitations

The area shapefile and model coefficient files listed in [Required runtime assets](#required-runtime-assets) are pending addition to this repository. Preprocessing requires the shapefile, and index calculation requires the coefficient files.

Spatial clipping currently retains the source workflow's SIRGAS 2000 (`EPSG:4989`) and longitude assumptions. Verify them against the complete SACZ shapefile before validating scientific output.

## Scientific reference

Nielsen DM, Belém AL, Marton E, Cataldi M (2019). Dynamics-based regression models for the South Atlantic Convergence Zone. *Climate Dynamics*, 52, 5527–5553. [Article](https://link.springer.com/article/10.1007/s00382-018-4460-4) · [DOI](https://doi.org/10.1007/s00382-018-4460-4).
