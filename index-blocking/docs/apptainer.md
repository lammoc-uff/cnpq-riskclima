# Atmospheric blocking index workflow

This document describes the workflow used to generate monthly climatologies
and daily atmospheric blocking index series from ERA5 and CMIP6 data.

The blocking criterion follows Cataldi et al. (2024) and is based on the
simultaneous occurrence of:

- positive relative vorticity at 850 hPa;
- positive relative vorticity at 500 hPa; and
- positive geopotential or geopotential-height anomaly at 500 hPa.

These conditions must persist for at least a defined number of consecutive
days (`PERSISTENCE_DAYS`). Once the persistence threshold is reached, the
initial days of the event are retroactively classified as blocking.

The workflow is implemented separately for ERA5 and CMIP6 because the datasets
differ in data access, variable representation, coordinate conventions, and
storage format.

## Workflow overview

For both data sources, the processing is divided into two main stages:

1. generation of the monthly reference climatology; and
2. calculation of the daily atmospheric blocking series.

```text
ERA5
  |
  +-- generate_era5_climatology.py
  |      |
  |      +-- monthly geopotential and vorticity climatologies
  |
  +-- generate_era5_blocking_series.py
         |
         +-- regional daily blocking series


CMIP6
  |
  +-- cmip6_generate_climatology.py
  |      |
  |      +-- monthly 500 hPa geopotential-height climatology
  |
  +-- cmip6_blocking_series.py
         |
         +-- regional daily blocking series
```

## ERA5 climatology

Script: `generate_era5_climatology.py`

This script generates the monthly ERA5 climatological fields required by the
atmospheric blocking workflow.

ERA5 monthly means are downloaded through the Copernicus Climate Data Store
(CDS) API for the selected reference period. The following pressure-level
variables are retrieved:

- geopotential (`z`);
- zonal wind (`u`); and
- meridional wind (`v`).

The pressure levels used are 500 and 850 hPa.

The processing consists of five main steps:

1. downloading ERA5 monthly means, one file per year;
2. concatenating the yearly files into a single monthly dataset;
3. extracting geopotential at 500 hPa;
4. calculating relative vorticity at 500 and 850 hPa from the horizontal wind
   components using MetPy; and
5. calculating the 12-month climatology with CDO `ymonmean`.

### Reference period

The reference period is configured using:

```python
CLIM_PERIOD = "60_90"
START_YEAR = 1960
END_YEAR = 1990
```

`CLIM_PERIOD` is used as a label in the output filenames. The start and end
years define the actual climatological period.

### Geographic domain

The default ERA5 download domain is defined as:

```python
AREA = [10, -70, -35, -30]
```

following the CDS convention:

```text
[north, west, south, east]
```

### ERA5 geopotential

ERA5 geopotential (`z`) is expressed in `m² s⁻²`. In this workflow, the
geopotential field is kept in its original units because the daily ERA5
blocking calculation uses the same variable and unit convention.

### Outputs

Intermediate and climatological files are written under:

```text
climatology_data/
```

The main climatology outputs are:

```text
clima_gz_{CLIM_PERIOD}.nc
clima_vort_{CLIM_PERIOD}.nc
```

`clima_gz_{CLIM_PERIOD}.nc` contains the monthly 500 hPa geopotential
climatology.

`clima_vort_{CLIM_PERIOD}.nc` contains monthly relative vorticity at 500 and
850 hPa.

Each climatology contains 12 monthly values, one for each calendar month.

### Requirements

The ERA5 climatology workflow requires Python packages including:

```text
cdsapi
xarray
netCDF4
metpy
```

CDO is also required for the monthly climatological averaging step.

## ERA5 atmospheric blocking series

Script: `generate_era5_blocking_series.py`

This script calculates the historical daily atmospheric blocking series from
ERA5 pressure-level data.

The workflow downloads daily ERA5 zonal wind, meridional wind, and geopotential
fields at 500 and 850 hPa and calculates the variables required by the blocking
criterion.

### Variables

The blocking criterion uses:

```text
vort850      relative vorticity at 850 hPa
vort500      relative vorticity at 500 hPa
anom_gz500   geopotential anomaly at 500 hPa
```

Relative vorticity is calculated from the ERA5 `u` and `v` wind components
using MetPy.

The 500 hPa geopotential anomaly is calculated as:

```text
daily ERA5 geopotential - monthly ERA5 geopotential climatology
```

Both terms are expressed in `m² s⁻²`.

### Blocking criterion

A day satisfies the atmospheric configuration required for blocking when:

```text
vort850 > 0
AND
vort500 > 0
AND
anom_gz500 > 0
```

The conditions must persist for at least:

```python
PERSISTENCE_DAYS = 3
```

consecutive days.

When the persistence threshold is reached, all days in the initial persistence
window are classified as blocking. Persistence is evaluated continuously and
is not reset at month or year boundaries.

### Climatology selection

The monthly geopotential climatology must be generated before running the
blocking-series script.

The selected climatology is controlled by:

```python
CLIM_PERIOD = "90_20"
```

and the expected climatology file is:

```text
climatology_data/clima_gz_{CLIM_PERIOD}.nc
```

The climatology period used to calculate the anomalies should therefore match
the intended reference period for the analysis.

### Processing strategy

The daily ERA5 data are processed one year at a time to reduce memory use.

For each year, the script:

1. loads only the selected year's temporal slice;
2. calculates relative vorticity at 850 hPa;
3. calculates relative vorticity at 500 hPa;
4. calculates the 500 hPa geopotential anomaly;
5. calculates spatial means for each blocking region; and
6. keeps only the resulting one-dimensional daily series before continuing to
   the next year.

The yearly series are concatenated only after all years have been processed.

### Geographic regions

The blocking series is calculated for seven predefined domains:

```text
total
norte
norte_h1
norte_h2
sul
sul_h1
sul_h2
```

The domains cover the region between approximately 10°S–25°S and
40°W–60°W and divide it into northern, southern, and longitudinal subsectors.

### Outputs

Results are written to:

```text
historical_output/{CLIM_PERIOD}/
```

For each region, the script generates:

```text
{area}_vars.csv
```

containing:

```text
date
vort850
vort500
anom_gz500
```

The final binary daily blocking series is stored in:

```text
daily_blocking_series.csv
```

where each regional column contains:

```text
0 = no blocking
1 = blocking
```

## CMIP6 climatology

Script: `cmip6_generate_climatology.py`

This script generates the monthly 500 hPa geopotential-height climatology used
by the CMIP6 atmospheric blocking workflow.

Unlike ERA5, CMIP6 geopotential height is read from the `zg` variable, which is
already expressed in metres. No conversion from geopotential to geopotential
height is therefore required.

For the selected CMIP6 model, the script:

1. locates the historical `zg` Zarr store;
2. selects the reference climatology period;
3. selects the pressure level closest to 500 hPa;
4. groups the daily fields by calendar month; and
5. calculates the long-term monthly mean.

### Configuration

The model and climatology period are configured using:

```python
SOURCE_ID = "ACCESS-CM2"

CLIM_START = 1980
CLIM_END = 2010
CLIM_LABEL = "80_10"
```

The local directory containing the CMIP6 Zarr stores must also be configured:

```python
CMIP6_DATA_DIR = Path("/path/to/CMIP6")
```

The expected directory structure is:

```text
{SOURCE_ID}/{EXPERIMENT_ID}/day/{variable_id}/{grid_label}/*.zarr
```

The climatology is always calculated from the model's `historical` experiment.

### Zarr selection

When multiple Zarr stores are available, the workflow gives priority to:

```text
ensemble_mean.zarr
ensemble_all.zarr
first available member-*.zarr
```

Candidate files are checked to ensure that pressure-level data are available
before they are used.

### Output

The monthly geopotential-height climatology is stored under:

```text
climatology_data/
```

with a filename following the pattern:

```text
{SOURCE_ID}_clima_zg500_{CLIM_LABEL}.nc
```

The output variable is:

```text
zg
```

with dimensions corresponding to:

```text
month
lat
lon
```

and 12 monthly climatological values.

The CMIP6 Zarr datasets used during development are Zarr v3 and therefore
require Python 3.11 or later and Zarr 3.x.

## CMIP6 atmospheric blocking series

Script: `cmip6_blocking_series.py`

This script calculates daily atmospheric blocking series from CMIP6
pressure-level fields.

The method follows the same blocking criterion used for ERA5, but the CMIP6
workflow accounts for differences in data format, variable names, coordinates,
and geopotential representation.

### Variables

The required CMIP6 variables are:

```text
ua   zonal wind
va   meridional wind
zg   geopotential height
```

Relative vorticity at 850 and 500 hPa is derived from `ua` and `va` using
MetPy.

The 500 hPa geopotential-height anomaly is calculated as:

```text
daily zg500 - monthly model-specific zg500 climatology
```

Because CMIP6 `zg` is already expressed as geopotential height in metres, no
unit conversion is required.

### Model-specific climatology

The CMIP6 blocking series must use a climatology calculated from the same
climate model.

The climatology file is expected to follow the pattern:

```text
climatology_data/{SOURCE_ID}_clima_zg500_{CLIM_LABEL}.nc
```

It should be generated with:

```text
cmip6_generate_climatology.py
```

before the blocking-series calculation is run.

### Configuration

The main processing options are:

```python
SOURCE_ID = "BCC-CSM2-MR"
EXPERIMENT_ID = "ssp585"
YEARS = list(range(2015, 2051))

PERSISTENCE_DAYS = 3
CLIM_LABEL = "80_10"
```

`CLIM_LABEL` must correspond to the climatology generated for the selected
model.

The local CMIP6 Zarr directory is configured with:

```python
CMIP6_DATA_DIR = Path("/path/to/CMIP6")
```

### Processing strategy

CMIP6 data are processed one year at a time.

For each year, the script:

1. selects `ua` and `va` at 850 hPa;
2. selects `ua` and `va` at 500 hPa;
3. calculates relative vorticity at both pressure levels;
4. selects `zg` at 500 hPa;
5. matches each day with the corresponding monthly climatology;
6. calculates the daily 500 hPa geopotential-height anomaly;
7. calculates spatial means for each blocking region; and
8. retains only the resulting one-dimensional regional series.

The three-dimensional atmospheric fields are discarded between years to limit
memory use.

### Blocking criterion

For each region, blocking is identified when:

```text
vort850 > 0
AND
vort500 > 0
AND
anom_zg500 > 0
```

for at least `PERSISTENCE_DAYS` consecutive days.

Once the persistence criterion is satisfied, the initial days of the event are
retroactively marked as blocking.

### Geographic regions

The CMIP6 calculation uses the same seven regional domains as the ERA5
workflow:

```text
total
norte
norte_h1
norte_h2
sul
sul_h1
sul_h2
```

### Outputs

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

The consolidated daily blocking series is stored in:

```text
daily_blocking_series.csv
```

Each regional column contains binary blocking indicators:

```text
0 = no blocking
1 = blocking
```

## ERA5 and CMIP6 differences

Although the same blocking criterion is applied to both datasets, some
preprocessing steps differ.

| Feature | ERA5 | CMIP6 |
| --- | --- | --- |
| Data access | CDS API | Local Zarr stores |
| Wind variables | `u`, `v` | `ua`, `va` |
| 500 hPa height variable | `z` | `zg` |
| Height units | Geopotential (`m² s⁻²`) | Geopotential height (`m`) |
| Climatology | ERA5 reference-period climatology | Model-specific historical climatology |
| Main storage format | NetCDF | Zarr |
| Coordinates | `latitude`, `longitude` | typically `lat`, `lon` |

The blocking condition itself remains equivalent because each daily anomaly is
calculated relative to a climatology expressed in the same units and based on
the same data source.

## Recommended execution order

### ERA5

Run:

```bash
python generate_era5_climatology.py
```

and then:

```bash
python generate_era5_blocking_series.py
```

### CMIP6

Run:

```bash
python cmip6_generate_climatology.py
```

and then:

```bash
python cmip6_blocking_series.py
```

The climatology-generation step must be completed before calculating the
corresponding blocking series.