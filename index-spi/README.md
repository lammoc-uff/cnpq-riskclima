# RiskClima SPI

This directory contains one Python package, `riskclima-spi`, with workflows for two data sources:

- [CMIP6](cmip6/README.md)
- [ERA5](era5/README.md)

The workflows convert source precipitation to monthly accumulation, fit a configured distribution over a calibration period, and calculate the Standardized Precipitation Index (SPI) with [xclim](https://xclim.readthedocs.io/en/stable/indices.html#xclim.indices.standardized_precipitation_index). CMIP6 uses prepared local NetCDF or Zarr data; ERA5 downloads configured CDS data on every run.

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

From the repository root, create the Python 3.12 environment and local configuration:

```bash
cd index-spi
make install
cp .env.example .env
```

The direct uv equivalent is:

```bash
uv sync --frozen
```

Both installation commands use the locked dependencies and create `.venv`. Activate it for direct Python commands:

```bash
source .venv/bin/activate
```

## Configure

`.env` is the operational configuration. Every entry point reads it at startup and exposes no command-line overrides. Relative paths resolve from the current working directory, so run commands from `index-spi`.

`.env.example` contains all supported settings:

```dotenv
# Shared SPI calculation
SPI_SCALE_MONTHS=1
SPI_DISTRIBUTION=gamma
SPI_METHOD=APP
SPI_FLOC=0

# Shared NetCDF output
NETCDF_ENGINE=netcdf4
NETCDF_FORMAT=NETCDF4
NETCDF_COMPRESSION=true
NETCDF_COMPLEVEL=4

# Shared metadata
METADATA_CREATORS="Marcio Cataldi <mcataldi@id.uff.br>"
METADATA_INSTITUTION="Climate System Monitoring and Modeling Laboratory (LAMMOC), Universidade Federal Fluminense (UFF), Niteroi, Brazil"
METADATA_PROJECT=RiskClima
METADATA_LICENSE=CC-BY-4.0
METADATA_REFERENCES=https://riskclima.com.br/
METADATA_REPOSITORY=https://github.com/lammoc-uff/cnpq-riskclima
METADATA_CONVENTIONS=CF-1.10
METADATA_PROCESSING_LEVEL="Processed data"

# Shared logging
LOG_LEVEL=INFO
LOG_FORMAT="%(levelname)s %(name)s: %(message)s"

# CMIP6
CMIP6_INPUT_FILE=/path/to/preprocessed_cmip6_precipitation.zarr
CMIP6_CALIBRATION_INPUT_FILE=/path/to/preprocessed_historical_precipitation.zarr
CMIP6_MODEL=ACCESS-CM2
CMIP6_EXPERIMENT=ssp245
CMIP6_MEMBER=r1i1p1f1
CMIP6_GRID=gn
CMIP6_PRECIPITATION_VARIABLE=pr
CMIP6_TIME_DIMENSION=time
CMIP6_LATITUDE_DIMENSION=lat
CMIP6_LONGITUDE_DIMENSION=lon
CMIP6_CALIBRATION_START=1961-01-01
CMIP6_CALIBRATION_END=1990-12-31
CMIP6_APPLICATION_START=2015-01-01
CMIP6_APPLICATION_END=2050-12-31
CMIP6_OUTPUT_DIRECTORY=cmip6/results
CMIP6_OUTPUT_TEMPLATE=spi{scale_months}_{model}_{experiment}_{member}_{grid}_{start}_{end}.nc

# CDS credentials used for every ERA5 run
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=
CDSAPI_CONFIG_FILE=~/.cdsapirc

# ERA5 acquisition
ERA5_DATASET=reanalysis-era5-single-levels-monthly-means
ERA5_PRODUCT_TYPE=monthly_averaged_reanalysis
ERA5_REQUEST_VARIABLE=total_precipitation
ERA5_DOWNLOAD_START=1940-01-01
ERA5_DOWNLOAD_END=2026-07-01
ERA5_TIME=00:00
ERA5_DATA_FORMAT=netcdf
ERA5_DOWNLOAD_FORMAT=unarchived
ERA5_LATITUDE_MIN=-70
ERA5_LATITUDE_MAX=20
ERA5_LONGITUDE_MIN=-120
ERA5_LONGITUDE_MAX=-5
ERA5_RAW_FILE_TEMPLATE=era5/raw_data/era5_tp_monthly_{start}_{end}.nc
ERA5_SPATIAL_CHUNK=32
ERA5_DASK_WORKERS=1

# ERA5 input, periods, and output
ERA5_PRECIPITATION_VARIABLE=tp
ERA5_TIME_DIMENSION=valid_time
ERA5_LATITUDE_DIMENSION=latitude
ERA5_LONGITUDE_DIMENSION=longitude
ERA5_CALIBRATION_START=1961-01-01
ERA5_CALIBRATION_END=1990-12-31
ERA5_APPLICATION_START=1940-01-01
ERA5_APPLICATION_END=2026-07-01
ERA5_OUTPUT_DIRECTORY=era5/results
ERA5_OUTPUT_TEMPLATE=spi{scale_months}_era5_{start}_{end}.nc
```

For CMIP6, set both input paths plus the model, experiment, member, and grid identity. The calibration and application latitude/longitude coordinates must match exactly. Historical ensemble means are represented explicitly with `CMIP6_MEMBER=ensemble_mean`; identity is not inferred from paths. For ERA5, `CDSAPI_URL` always selects the endpoint. A nonblank `CDSAPI_KEY` takes priority even if `CDSAPI_CONFIG_FILE` does not exist; otherwise only `key` is read from that file and any file `url` is ignored. Every ERA5 run downloads and atomically replaces the exact date-based raw `.nc` path after monthly-coverage and exact-grid validation, cleaning temporary parts on success or failure. SPI calculation uses complete-time Dask chunks, 32-cell spatial chunks, one worker, and a delayed NetCDF write.

## Run

Direct Python commands require the activated environment:

```bash
python cmip6/scripts/run_spi_cmip6.py
python era5/scripts/run_spi_era5.py
```

The installed entry points can be run through uv without activation:

```bash
uv run --frozen riskclima-spi-cmip6
uv run --frozen riskclima-spi-era5
```

Equivalent Make targets also use uv:

```bash
make run_spi_cmip6
make run_spi_era5
```

Run project checks with:

```bash
make check
```

`make check` checks formatting, runs Ruff and Pyrefly, and executes the test suite with coverage.

## Docker

Build the CPU runtime image from `index-spi`:

```bash
docker build --target runtime -t riskclima-spi:local .
```

The image defaults to the CMIP6 entry point. Mount the project so `.env`, inputs, raw downloads, and outputs remain on the host:

```bash
docker run --rm \
  --env-file .env \
  --volume "$PWD:/work" \
  --workdir /work \
  riskclima-spi:local
```

Select ERA5 by overriding the image command:

```bash
docker run --rm \
  --env-file .env \
  --volume "$PWD:/work" \
  --workdir /work \
  riskclima-spi:local \
  riskclima-spi-era5
```

All configured paths must be visible inside the container. If ERA5 credentials come from `CDSAPI_CONFIG_FILE` instead of `CDSAPI_KEY`, bind that file and use its container path in `.env`.

## Apptainer

Convert the local Docker image to SIF:

```bash
apptainer build riskclima-spi.sif docker-daemon://riskclima-spi:local
```

Run either entry point with the project bound at `/work`:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-cmip6

apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-era5
```

See [`docs/apptainer.md`](docs/apptainer.md) for separate configuration and credential bindings. Docker and Apptainer commands are usage instructions; they were not verified as part of this documentation update.

## Notebook

The source notebooks provide simplified Colab-oriented references for the same workflows:

- [`cmip6/notebooks/spi.ipynb`](cmip6/notebooks/spi.ipynb)
- [`era5/notebooks/spi.ipynb`](era5/notebooks/spi.ipynb)

The packaged CMIP6 and ERA5 entry points described above are the operational interfaces.
