# RiskClima Atmospheric Blocking Index

This directory contains workflows to generate monthly climatologies and
calculate daily atmospheric blocking index series using two data sources:

- **ERA5**
- **CMIP6**

Both workflows apply the atmospheric blocking criterion described by
Cataldi et al. (2024), based on the persistence of positive relative
vorticity at 850 and 500 hPa together with a positive 500 hPa
geopotential or geopotential-height anomaly.

The ERA5 and CMIP6 workflows use different preprocessing procedures because
the datasets differ in data access, variable names, units, coordinate
conventions, and storage format.

For a detailed description of the methodology and processing steps, see
[`docs/blocking-index.md`](docs/blocking-index.md).

## Workflow

For each data source, the processing is divided into two stages:

1. generation of the monthly reference climatology; and
2. calculation of the daily atmospheric blocking series.

### ERA5

1. `generate_era5_climatology.py`
   - downloads ERA5 monthly means;
   - extracts 500 hPa geopotential;
   - calculates relative vorticity at 500 and 850 hPa; and
   - generates monthly climatological fields for the selected reference period.

2. `generate_era5_blocking_series.py`
   - downloads daily ERA5 pressure-level fields;
   - calculates relative vorticity at 850 and 500 hPa;
   - calculates 500 hPa geopotential anomalies;
   - applies the blocking persistence criterion; and
   - saves regional daily blocking series.

### CMIP6

1. `cmip6_generate_climatology.py`
   - reads historical CMIP6 geopotential height from local Zarr stores;
   - selects the reference period and 500 hPa level; and
   - calculates the monthly 500 hPa geopotential-height climatology.

2. `cmip6_blocking_series.py`
   - reads CMIP6 zonal wind, meridional wind, and geopotential height;
   - calculates relative vorticity at 850 and 500 hPa;
   - calculates 500 hPa geopotential-height anomalies;
   - applies the blocking persistence criterion; and
   - saves regional daily blocking series.

## Blocking criterion

A day satisfies the atmospheric blocking condition when all three variables
are positive:

```text
vort850 > 0
AND
vort500 > 0
AND
anom500 > 0
```

where:

- `vort850` is relative vorticity at 850 hPa;
- `vort500` is relative vorticity at 500 hPa; and
- `anom500` is the 500 hPa geopotential anomaly for ERA5 or
  geopotential-height anomaly for CMIP6.

The conditions must persist for at least:

```python
PERSISTENCE_DAYS = 3
```

consecutive days.

Once the persistence threshold is reached, the initial days of the event are
retroactively classified as blocking.

## Geographic regions

The blocking index is calculated for seven predefined regions:

```text
total
norte
norte_h1
norte_h2
sul
sul_h1
sul_h2
```

These domains cover the blocking-analysis region between approximately
10°S–25°S and 40°W–60°W and divide it into northern, southern, and
longitudinal subsectors.

## Requirements

The workflows require a Python environment with packages including:

```text
numpy
pandas
xarray
metpy
cdsapi
netCDF4
```

Additional requirements depend on the data source.

### ERA5

The ERA5 climatology workflow also requires CDO:

```text
cdo
```

ERA5 data are downloaded through the Copernicus Climate Data Store (CDS) API.
A valid CDS API configuration is therefore required.

### CMIP6

The CMIP6 workflows expect pressure-level data to be available locally as
Zarr stores.

The Zarr datasets used during development are Zarr v3 and therefore require:

```text
Python >= 3.11
zarr >= 3
```

## Configure

### ERA5 climatology

Set the reference period in `generate_era5_climatology.py`:

```python
CLIM_PERIOD = "60_90"
START_YEAR = 1960
END_YEAR = 1990
```

The geographic domain is defined as:

```python
AREA = [10, -70, -35, -30]
```

using the CDS convention:

```text
[north, west, south, east]
```

### ERA5 blocking series

Set the processing period, persistence threshold, and climatology label in
`generate_era5_blocking_series.py`:

```python
START_YEAR = 1960
END_YEAR = 2025

PERSISTENCE_DAYS = 3
CLIM_PERIOD = "90_20"
```

The climatology identified by `CLIM_PERIOD` must already exist before the
blocking series is calculated.

### CMIP6 climatology

Set the model, reference period, and local CMIP6 data directory in
`cmip6_generate_climatology.py`:

```python
SOURCE_ID = "ACCESS-CM2"

CLIM_START = 1980
CLIM_END = 2010
CLIM_LABEL = "80_10"

CMIP6_DATA_DIR = Path("/path/to/CMIP6")
```

The expected CMIP6 directory structure is:

```text
{SOURCE_ID}/{EXPERIMENT_ID}/day/{variable_id}/{grid_label}/*.zarr
```

The climatology is calculated from the model's `historical` experiment.

### CMIP6 blocking series

Set the model, experiment, years, persistence threshold, climatology label,
and local Zarr directory in `cmip6_blocking_series.py`:

```python
SOURCE_ID = "BCC-CSM2-MR"
EXPERIMENT_ID = "ssp585"

YEARS = list(range(2015, 2051))

PERSISTENCE_DAYS = 3
CLIM_LABEL = "80_10"

CMIP6_DATA_DIR = Path("/path/to/CMIP6")
```

`CLIM_LABEL` must match the climatology generated for the selected model.

## Run

The climatology-generation step must be completed before calculating the
corresponding blocking series.

### ERA5

Generate the climatology:

```bash
python generate_era5_climatology.py
```

Then calculate the daily blocking series:

```bash
python generate_era5_blocking_series.py
```

### CMIP6

Generate the model-specific climatology:

```bash
python cmip6_generate_climatology.py
```

Then calculate the daily blocking series:

```bash
python cmip6_blocking_series.py
```

## Outputs

### ERA5 climatology

Climatology files are written under:

```text
climatology_data/
```

Main outputs:

```text
clima_gz_{CLIM_PERIOD}.nc
clima_vort_{CLIM_PERIOD}.nc
```

The geopotential climatology contains monthly 500 hPa ERA5 geopotential in
`m² s⁻²`.

The vorticity climatology contains monthly relative vorticity at 500 and
850 hPa.

### ERA5 blocking series

Results are written to:

```text
historical_output/{CLIM_PERIOD}/
```

For each region:

```text
{area}_vars.csv
```

contains the daily predictors:

```text
date
vort850
vort500
anom_gz500
```

The consolidated binary series is saved as:

```text
daily_blocking_series.csv
```

with:

```text
0 = no blocking
1 = blocking
```

### CMIP6 climatology

The monthly model-specific climatology is written under:

```text
climatology_data/
```

using the filename:

```text
{SOURCE_ID}_clima_zg500_{CLIM_LABEL}.nc
```

The output contains monthly 500 hPa geopotential height (`zg`) in metres.

### CMIP6 blocking series

Results are written under:

```text
output/{SOURCE_ID}/{EXPERIMENT_ID}/
```

For each region:

```text
{area}_vars.csv
```

contains:

```text
date
vort850
vort500
anom_zg500
```

The consolidated daily series is saved as:

```text
daily_blocking_series.csv
```

## ERA5 and CMIP6 differences

| Feature | ERA5 | CMIP6 |
| --- | --- | --- |
| Data access | CDS API | Local Zarr stores |
| Wind variables | `u`, `v` | `ua`, `va` |
| 500 hPa variable | `z` | `zg` |
| Height representation | Geopotential (`m² s⁻²`) | Geopotential height (`m`) |
| Climatology | ERA5 reference-period climatology | Model-specific historical climatology |
| Main storage format | NetCDF | Zarr |
| Typical coordinates | `latitude`, `longitude` | `lat`, `lon` |

Although the preprocessing differs, the blocking criterion is applied
consistently within each dataset because the daily anomaly and its reference
climatology use the same variable and units.

## Documentation

See [`docs/blocking-index.md`](docs/blocking-index.md) for additional details
on:

- the atmospheric blocking criterion;
- ERA5 climatology generation;
- ERA5 blocking-series processing;
- CMIP6 model-specific climatology;
- CMIP6 blocking-series processing;
- geographic regions;
- data formats and units; and
- differences between the ERA5 and CMIP6 workflows.