# RiskClima SACZ Index

This directory contains workflows to preprocess atmospheric data and calculate
the South Atlantic Convergence Zone (SACZ) index using two data sources:

- **ERA5**
- **CMIP6**

Both workflows prepare the atmospheric predictors required by the SACZ
statistical model and calculate a daily index for the three SACZ regions:
`AB`, `C`, and `DE`.

The ERA5 and CMIP6 workflows use different preprocessing procedures because
the datasets differ in format and data access, but both produce the same set
of atmospheric input variables used by the index calculation.

## Workflow

The processing is divided into two stages for each data source.

### ERA5

1. `era5_download_process_sacz_index.py`
   - downloads the required ERA5 pressure-level fields;
   - calculates daily means;
   - converts geopotential to geopotential height;
   - computes divergence and vorticity;
   - merges daily fields into yearly files; and
   - calculates spatial means over the SACZ index regions.

2. `main_sacz_index.py`
   - reads the preprocessed ERA5 atmospheric time series;
   - applies the SACZ statistical model; and
   - saves the daily SACZ index for regions `AB`, `C`, and `DE`.

### CMIP6

1. `cmip6_process_sacz_index.py`
   - reads the required CMIP6 atmospheric fields from local Zarr stores;
   - extracts the required pressure levels;
   - computes divergence and vorticity;
   - calculates spatial means over the SACZ index regions; and
   - saves the resulting time series as CSV files.

2. `main_sacz_index_cmip6.py`
   - reads the preprocessed CMIP6 time series;
   - applies the SACZ statistical model; and
   - saves the daily SACZ index for regions `AB`, `C`, and `DE`.

For a detailed description of the preprocessing and statistical calculation,
see [`docs/sacz-index.md`](docs/sacz-index.md).

## Atmospheric predictors

The SACZ index uses the following atmospheric variables:

| Output | Variable | Pressure level |
| --- | --- | ---: |
| `div200.csv` | Horizontal divergence | 200 hPa |
| `div850.csv` | Horizontal divergence | 850 hPa |
| `hgt500.csv` | Geopotential height | 500 hPa |
| `omega500.csv` | Vertical pressure velocity | 500 hPa |
| `uwnd200.csv` | Zonal wind | 200 hPa |
| `uwnd850.csv` | Zonal wind | 850 hPa |
| `vwnd200.csv` | Meridional wind | 200 hPa |
| `vwnd850.csv` | Meridional wind | 850 hPa |
| `vort200.csv` | Relative vorticity | 200 hPa |

For CMIP6 models where 200 hPa is unavailable, the preprocessing workflow can
use 250 hPa as a proxy without vertical interpolation. The output filenames
retain the 200 hPa reference used by the index.

## Requirements

The workflows require a Python environment with the packages used by the
processing scripts, including:

```text
numpy
pandas
xarray
geopandas
metpy
```

Additional requirements depend on the data source.

### ERA5

The ERA5 preprocessing workflow also requires:

- [CDO](https://code.mpimet.mpg.de/projects/cdo)
- `gsutil`

ERA5 fields are downloaded from the public ARCO ERA5 Google Cloud bucket.

### CMIP6

The CMIP6 workflow expects the atmospheric fields to be available locally as
Zarr stores.

The Zarr datasets used during development are Zarr v3 and therefore require:

```text
Python >= 3.11
zarr >= 3
```

## Configure

### ERA5

The processing year is provided through the `ERA5_YEAR` environment variable.

Example:

```bash
export ERA5_YEAR=2020
```

The ERA5 preprocessing script expects the project directories for data,
coefficient files, and SACZ region polygons to be available relative to the
project root.

### CMIP6

Set the model, experiment, years, and local CMIP6 data directory in
`cmip6_process_sacz_index.py`.

Example:

```python
SOURCE_ID = "BCC-CSM2-MR"
EXPERIMENT_ID = "ssp245"
YEARS = list(range(2015, 2051))

CMIP6_DATA_DIR = Path("/path/to/CMIP6_SACZ")
```

The expected CMIP6 input structure is:

```text
{SOURCE_ID}/{EXPERIMENT_ID}/day/{variable_id}/{grid_label}/*.zarr
```

## Run

### ERA5

Set the processing year and run the preprocessing workflow:

```bash
export ERA5_YEAR=2020
python era5_download_process_sacz_index.py
```

Then calculate the SACZ index:

```bash
python main_sacz_index.py
```

### CMIP6

First preprocess the CMIP6 fields:

```bash
python cmip6_process_sacz_index.py
```

Then calculate the SACZ index:

```bash
python main_sacz_index_cmip6.py
```

## Outputs

### ERA5

Preprocessed atmospheric time series:

```text
data/input/era5/{YEAR}/
```

Intermediate statistical-model outputs:

```text
data/intermediatives/era5/{YEAR}/
```

Final SACZ index:

```text
output/era5/{YEAR}/
    AB.csv
    C.csv
    DE.csv
```

### CMIP6

Preprocessed atmospheric time series:

```text
data/input/cmip6/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/
```

Intermediate statistical-model outputs:

```text
data/intermediatives/cmip6/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/
```

Final SACZ index:

```text
output/cmip6/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/
    AB.csv
    C.csv
    DE.csv
```

## Statistical model

For both ERA5 and CMIP6, the SACZ index calculation follows four stages:

1. normalization of the atmospheric predictors;
2. calculation of principal component scores;
3. linear combination using regional logistic-regression coefficients; and
4. logistic transformation of the resulting score to the `[0, 1]` interval.

The model coefficients are stored under `coefs/` and are shared by the ERA5
and CMIP6 calculation workflows.

## Documentation

See [`docs/sacz-index.md`](docs/sacz-index.md) for a detailed description of:

- ERA5 preprocessing;
- CMIP6 preprocessing;
- pressure-level handling;
- input and output structure; and
- the SACZ statistical calculation workflow.