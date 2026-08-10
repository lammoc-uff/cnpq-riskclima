# CNPq RiskClima

RiskClima develops georeferenced climate-risk products for Brazil. The project combines climate observations and reanalyses, CMIP6 projections, regional climate analysis, hydrological and geomorphological applications, socioenvironmental indicators, and data-driven methods.

Project website: <https://riskclima.com.br>
Repository: <https://github.com/lammoc-uff/cnpq-riskclima>

## Project components

| Component | Purpose |
|---|---|
| [`cmip6-downloader`](cmip6-downloader/README.md) | Compares catalogs, filters datasets, downloads CMIP6 members, preprocesses fields, and writes Zarr stores. |
| [`index-xhwi`](index-xhwi/README.md) | Computes the Extreme Heatwave Index from ERA5, ERA5-Land, and CMIP6 data. |
| [`index-spi`](index-spi/README.md) | Computes the Standardized Precipitation Index from CMIP6 and ERA5 precipitation. |
| [`index-blocking`](index-blocking/README.md) | Generates blocking climatologies and daily atmospheric blocking series from ERA5 and CMIP6. |
| [`index-sacz`](index-sacz/README.md) | Preprocesses atmospheric predictors and computes the South Atlantic Convergence Zone index from ERA5 and CMIP6. |

Each index is an independent Python project with its own environment, configuration, lockfile, Makefile, and operational documentation. Run commands from the directory of the component being used.

## General workflow

1. Use `cmip6-downloader` to identify and prepare the CMIP6 datasets required by an index.
2. Configure the selected index by copying its `.env.example` to `.env`.
3. Install that index with its documented `make install` or `uv sync --frozen` command.
4. Run its source-specific processing and index stages independently.
5. Integrate the generated climate indicators with other RiskClima hazard, vulnerability, and risk products.

The index READMEs are the source of truth for paths, periods, variables, credentials, external tools, and output schemas.

## Project information

RiskClima is coordinated by Marcio Cataldi and is associated with the Climate System Monitoring and Modeling Laboratory (LAMMOC) at Universidade Federal Fluminense (UFF), in Niteroi, Brazil. The project also has an institutional association with COPPE/UFRJ.

The project name is RiskClima. Its main reference is <https://riskclima.com.br>, and the source repository is <https://github.com/lammoc-uff/cnpq-riskclima>. Generated data products use the project’s documented processing conventions and are distributed under the CC BY 4.0 license when the corresponding product configuration specifies it.

Dataset-specific scientific profiles, input sources, calibration periods, processing parameters, creators, and other product metadata remain documented in each component’s `.env.example` and README.

## Utilities

The [`utils/convert_zarr_nc`](utils/convert_zarr_nc/README.md) directory contains a small reference utility for converting Zarr datasets to NetCDF and NetCDF datasets to Zarr. It exists as documentation and practical support because these conversions are occasionally needed while preparing and inspecting climate-data inputs.

## Reproducibility

Each maintained index uses Python 3.12, `uv`, and a versioned `uv.lock`. Climate inputs, credentials, intermediate files, and generated results are kept outside the Python package and are configured through `.env` where required. Docker and Apptainer instructions are provided by the components that support those execution modes.

The repository does not contain credentials or general climate-data archives. Consult each component README for the required external data and system tools, including CDS API access, CDO, Google Cloud tools, and local CMIP6 Zarr stores.

## Maintainers and contact

RiskClima is associated with LAMMOC/UFF and COPPE/UFRJ and is developed by a multidisciplinary team working on climate modeling, hydrology, socioenvironmental vulnerability, risk analysis, and applied data science.

Official contact: <mcataldi@id.uff.br>
