# CMIP6 Downloader

This repository provides a reproducible workflow to compare CMIP6 cloud catalogs, filter target datasets, download selected members, preprocess them, save them as local Zarr stores, and optionally build ensemble products.

## What the project does

This project:

- compares AWS and Google CMIP6 catalogs
- filters catalog entries by model, experiment, table, variable, and grid
- opens remote CMIP6 Zarr datasets through ``fsspec``
- preprocesses time coordinates and spatial coordinates
- crops supported datasets to Brazil / extended South America
- saves each member incrementally in local Zarr blocks
- builds stacked and mean ensemble outputs

The catalog comparison script writes filtered catalogs and difference tables, while the main download script processes datasets group by group using parallel threads and writes logs for each group and for the full run. :contentReference[oaicite:5]{index=5} :contentReference[oaicite:6]{index=6} :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8}

## Why the project is useful

This project is useful when you need an organized and reproducible way to acquire and preprocess CMIP6 data from cloud-hosted catalogs for climate analyses.

It is particularly useful for:

- screening CMIP6 availability across providers
- reducing download scope through explicit metadata filters
- standardizing calendars and coordinates before analysis
- saving large datasets safely in blocks to avoid memory overload
- creating ensemble datasets for downstream workflows

The configuration centralizes source models, experiments, table IDs, variables, chunk size, calendar conversion, grouping fields, ensemble mode, and output naming. :contentReference[oaicite:9]{index=9} :contentReference[oaicite:10]{index=10} :contentReference[oaicite:11]{index=11} :contentReference[oaicite:12]{index=12}

## How users can get started with the project

### Repository structure

```
    cmip6-downloader/
    ├── catalog/
    ├── downloads/
    ├── filtered_catalog/
    ├── notebooks/
    ├── scripts/
    ├── env-cmip6-downloader.yml
    └── requirements.txt
```

### Main scripts

- ``scripts/compare_catalogs.py`` — compares AWS and Google catalogs and writes filtered and difference CSV files
- ``scripts/run_download.py`` — runs the download and preprocessing workflow group by group

### Main modules

- ``src/config.py`` — project paths, filters, grouping rules, chunking, and ensemble settings
- ``src/compare.py`` — catalog comparison logic
- ``src/downloader.py`` — main downloader class and grouped processing workflow
- ``src/filters.py`` — metadata normalization, filtering, and grouping helpers
- ``src/preprocessing.py`` — time cleanup, coordinate adjustment, and spatial cropping
- ``src/writer.py`` — block-wise Zarr writing and ensemble construction

### Environment setup

Create the environment with Conda:

    conda env create -f env-cmip6-downloader.yml
    conda activate env-cmip6-downloader

Or install dependencies with pip:

    pip install -r requirements.txt

The environment is centered on Python 3.13 and packages such as ``xarray``, ``zarr``, ``intake-esm``, ``fsspec``, ``s3fs``, ``gcsfs``, ``netcdf4``, ``numpy``, ``pandas``, and ``scipy``. :contentReference[oaicite:13]{index=13}

### Typical workflow

1. Place the CMIP6 catalog CSV files inside ``catalog/``.
2. Edit ``scripts/src/config.py`` to define:
   - ``source_ids``
   - ``experiment_ids``
   - ``table_ids``
   - ``variable_ids``
3. Run the catalog comparison:

    python scripts/compare_catalogs.py

4. Run the grouped download and preprocessing workflow:

    python scripts/run_download.py

5. Inspect outputs under ``downloads/``.

The current configuration targets selected CMIP6 models, experiments, table frequencies, and variables, and stores per-member outputs as ``member-{member_id}.zarr``. It can also generate ``ensemble_all.zarr`` and ``ensemble_mean.zarr``. :contentReference[oaicite:14]{index=14} :contentReference[oaicite:15]{index=15} :contentReference[oaicite:16]{index=16} :contentReference[oaicite:17]{index=17}

## Where users can get help with your project

Users can get help by:

- opening an issue in this repository
- reviewing the configuration in ``scripts/src/config.py``
- checking the CSV logs written to each group directory and to the global download directory
- inspecting the filtered catalogs and catalog difference outputs

The workflow writes a per-group catalog, a per-group log, and a global log file. :contentReference[oaicite:18]{index=18} :contentReference[oaicite:19]{index=19} :contentReference[oaicite:20]{index=20}

## Who maintains and contributes to the project

This project is maintained by:

- ``lammoc-uff``

Contributors may include:

- Galves, V. L. V.; Sancho, L.; da Fonseca Aguiar, L.; Esposte Coutinho, P.; Guida, A.; Cataldi, M. 2025.

## Scientific and technical overview

The workflow follows this logic:

1. load the selected catalog
2. normalize and filter CMIP6 metadata
3. group entries by ``source_id``, ``experiment_id``, ``table_id``, ``variable_id``, and ``grid_label``
4. open each remote Zarr store
5. preprocess time and coordinates
6. crop supported datasets to Brazil / extended South America
7. save local member datasets in time-sliced blocks
8. align members and build ensemble outputs

Time preprocessing converts ``CFTimeIndex`` calendars, removes duplicate timestamps, and crops SSP datasets to the 2015–2050 window when applicable. Coordinate preprocessing adjusts longitudes to ``[-180, 180]``, sorts one-dimensional latitude and longitude coordinates, and skips special handling for ``tos``. :contentReference[oaicite:21]{index=21}

## Input and output

### Input

- ``catalog/pangeo-cmip6_aws.csv``
- ``catalog/pangeo-cmip6_google.csv``

### Output

Typical outputs include:

- filtered catalog CSV files
- catalog difference CSV files
- per-group catalog and log files
- per-member local Zarr datasets
- ``ensemble_all.zarr``
- ``ensemble_mean.zarr``
- global download log

## Notes on notebooks

The notebooks currently appear to be exploratory or legacy materials rather than part of the operational workflow.

Suggested policy:

- keep only notebooks that document experiments still relevant to the project
- remove obsolete notebooks or move them to an archival folder
- avoid treating notebooks as the main execution interface when equivalent scripts already exist

## Affiliation

``Laboratory for Monitoring and Modeling of Climate Systems``
``Federal Fluminense University``

## Contact

``mcataldi@id.uff.br``
