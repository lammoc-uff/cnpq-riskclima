# ERA5 XHWI Workflow

This directory contains the ERA5-specific implementation of the XHWI workflow.

ERA5 is currently used as the reanalysis data source for computing hourly XHWI fields, daily indicators, monthly indicators, and combined NetCDF outputs over the configured spatial domain.

## Scope

The code in this module is responsible for:

- preprocessing ERA5 2 m temperature files into a Zarr store
- preprocessing ERA5 relative humidity files into a Zarr store
- loading the calibration dataset used to build the empirical CDF
- matching validation ERA5 temperature values to calibration probabilities
- computing hourly XHWI values
- generating daily and monthly indicator products
- writing Zarr and NetCDF outputs

## Directory Structure

```text
era5/
├── README.md
├── raw_data/
│   └── README.md
└── spatial/
    ├── results/
    │   └── README.md
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

## Inputs

Expected input files and stores are defined in `spatial/scripts/src/config/settings.py`.

The current naming convention uses `STARTYEAR_ENDYEAR` as the period placeholder.

Required inputs:

- `raw_data/temp_max_Brazil_1961-1990.nc`
- `raw_data/t2m_prev_STARTYEAR_ENDYEAR/`
- `raw_data/t2m_prev_STARTYEAR_ENDYEAR/zarr_store_t2m_br_STARTYEAR_ENDYEAR`
- `raw_data/r_prev_STARTYEAR_ENDYEAR/`
- `raw_data/r_prev_STARTYEAR_ENDYEAR/zarr_store_humidity_br_STARTYEAR_ENDYEAR`

The calibration variable name is configured through `TARGET_CAL_VAR` in `settings.py`.

## Configuration

Main configuration file:

```text
era5/spatial/scripts/src/config/settings.py
```

Important settings include:

- `PERIOD_NAME`: period label used in input and output paths
- `CALIB_PATH`: calibration NetCDF path
- `ZARR_T2M`: preprocessed ERA5 temperature Zarr store
- `ZARR_R`: preprocessed ERA5 relative humidity Zarr store
- `TARGET_CAL_VAR`: variable name in the calibration dataset
- `CHUNKS`: chunking policy used before writing outputs
- `OVERWRITE`: whether existing outputs should be overwritten
- `DAILY_VARIABLE`: postprocessing switch for daily or monthly combined NetCDF export

## Running The Workflow

Run commands from:

```bash
cd index-xhwi/era5/spatial/scripts
```

Preprocess ERA5 temperature:

```bash
python preprocess_t2m.py
```

Preprocess ERA5 relative humidity:

```bash
python preprocess_r.py
```

Run all months:

```bash
python run_all_months.py
```

Run a single month:

```bash
python run_single_month.py 7
```

Postprocess Zarr outputs into a combined NetCDF file:

```bash
python postprocess_aggregations.py
```

## Outputs

The ERA5 workflow writes outputs to:

```text
era5/spatial/results/
```

Expected output names:

- `xhwi_era5_STARTYEAR_ENDYEAR_br_month_{m}_zarr_store.zarr`
- `xhwi_era5_STARTYEAR_ENDYEAR_br_diary_ind_prod_{m}_zarr_store.zarr`
- `xhwi_era5_STARTYEAR_ENDYEAR_br_month_ind_prod_{m}_zarr_store.zarr`
- `xhwi_era5_STARTYEAR_ENDYEAR_br_daily_combined.nc`
- `xhwi_era5_STARTYEAR_ENDYEAR_br_monthly_combined.nc`

## Notes

- This module is ERA5-specific. Other data sources, such as CMIP6, should have their own module-level implementation.
- Shared project dependencies should live at the `index-xhwi` root when the project supports multiple data sources.
- Large raw and result files should normally remain outside version control.
