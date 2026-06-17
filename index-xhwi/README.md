# XHWI for Spatial Data

[![Python](https://img.shields.io/badge/python-3.10-blue.svg)]()
[![xarray](https://img.shields.io/badge/xarray-workflow-green.svg)]()
[![Dask](https://img.shields.io/badge/dask-enabled-orange.svg)]()
[![Status](https://img.shields.io/badge/status-active-success.svg)]()

**Paper reference:** 

- Development of a New Generalizable, Multivariate, and Physical-Body-Response-Based Extreme Heatwave Index
- https://www.mdpi.com/2073-4433/15/12/1541

## What the project does

This repository provides a reproducible workflow to compute the Extreme Heatwave Index XHWI for a given region of Earth.

- In the current project, the code is used with ERA5 data.
- The default computation domain corresponds to Brazil.

Note: Implementation of the code for other data sources is currently under development.

The pipeline:

- preprocesses hourly ERA5 temperature and relative humidity data into Zarr stores
- opens calibration and validation datasets
- computes the empirical cumulative distribution function ``CDF`` from calibration temperature data (daily maximum temperatures of a reference period)
- matches validation temperature values to calibration probabilities
- computes the XHWI field
- derives daily and monthly indicators
- exports hourly, daily, and monthly outputs to Zarr and NetCDF

## Why the project is useful

This project is useful for climate and environmental studies that require a reproducible and modular workflow for heatwave diagnostics based on long-term reanalysis data.

It supports:

- climate monitoring
- spatial analysis of heatwave conditions
- reproducible scientific workflows
- generation of outputs for undergraduate research, undergraduate theses, master's dissertations, doctoral theses, and scientific papers
- development of downstream climate risk applications

## How users can get started with the project

### Repository structure

```
era5/
├── raw_data/
└── spatial/
    ├── results/
    └── scripts/
        ├── postprocess_aggregations.py
        ├── preprocess_r.py
        ├── preprocess_t2m.py
        ├── run_all_months.py
        ├── run_single_month.py
        └── src/
            ├── cdf/
            ├── config/
            ├── features/
            ├── io/
            ├── pipeline/
            ├── preprocessing/
            └── utils/
```

### Main scripts

- ``preprocess_t2m.py`` — preprocess ERA5 2 m temperature files into a Zarr store
- ``preprocess_r.py`` — preprocess ERA5 relative humidity files into a Zarr store
- ``run_all_months.py`` — run the full XHWI pipeline for all months
- ``run_single_month.py`` — run the XHWI pipeline for one selected month
- ``postprocess_aggregations.py`` — combine daily or monthly outputs into NetCDF files

### Main modules

- ``src/cdf`` — empirical CDF computation and interpolation
- ``src/config`` — project settings and file paths
- ``src/features`` — XHWI computation and aggregated indicators
- ``src/io`` — loaders and writers
- ``src/pipeline`` — orchestration of the monthly and full pipeline
- ``src/preprocessing`` — preprocessing and Zarr writing
- ``src/utils`` — Dask client, logging, and encoding helpers

### Environment setup

This project uses:

- ``heatwaves_env.yml``
- ``requirements.txt``

Create the environment with Conda:

    conda env create -f heatwaves_env.yml
    conda activate heatwaves_env

Or install dependencies with pip:

    pip install -r requirements.txt

### Typical workflow

1. Preprocess temperature files:

    python preprocess_t2m.py

2. Preprocess relative humidity files:

    python preprocess_r.py

3. Run the full monthly pipeline:

    python run_all_months.py

4. Or run a single month:

    python run_single_month.py

5. Postprocess outputs into combined NetCDF files:

    python postprocess_aggregations.py

## Where users can get help with your project

Users can get help by:

- opening an issue in this repository
- contacting the project maintainers
- reviewing the configuration file in ``src/config/settings.py``
- checking the log files generated during execution

## Who maintains and contributes to the project

This project is maintained by:

- ``lammoc-uff``

## Contributors:

- Galves, V. L. V.; Cataldi, M.

## Scientific workflow overview

The processing chain follows this general logic:

1. ERA5 temperature and humidity files are preprocessed into Zarr stores
2. Calibration data are loaded from a NetCDF reference dataset
3. Validation ERA5 temperature values are matched to calibration CDF probabilities
4. The XHWI is computed from temperature exceedance and humidity
5. Daily and monthly indicators are generated
6. Outputs are written to Zarr and optionally combined into NetCDF

## Input and output

### Input

- calibration NetCDF dataset (using daily maximum temperatures of a reference period)
- ERA5 temperature Zarr store
- ERA5 relative humidity Zarr store

### Output

- hourly XHWI Zarr files
- daily indicator Zarr files
- monthly indicator Zarr files
- combined NetCDF outputs in ``era5/spatial/results``

## Main outputs

Expected outputs include:

- ``xhwi_era5_1950-2024_br_month_{m}_zarr_store.zarr``
- ``xhwi_era5_1950-2024_br_diary_ind_prod_{m}_zarr_store.zarr``
- ``xhwi_era5_1950-2024_br_month_ind_prod_{m}_zarr_store.zarr``
- combined daily NetCDF
- combined monthly NetCDF

## Citation

If you use this repository in research products, please cite:

- the reference paper of the XHWI index
- this repository
- ERA5 or the corresponding climate dataset used as input

## Affiliation

``Laboratory for Monitoring and Modeling of Climate Systems``

``Federal Fluminense University``

## Contact

- ``mcataldi@id.uff.br``
