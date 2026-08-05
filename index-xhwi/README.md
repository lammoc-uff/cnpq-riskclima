# RiskClima XHWI

This directory contains one Python package, `riskclima-xhwi`, with workflows for three data sources:

- [ERA5](era5/README.md)
- [ERA5-Land](era5land/README.md)
- [CMIP6](cmip6/README.md)

The workflows calculate monthly accumulated Extreme Heatwave Index (XHWI) fields with xarray and spatial PyTorch blocks. The scientific reference is [Development of a New Generalizable, Multivariate, and Physical-Body-Response-Based Extreme Heatwave Index](https://www.mdpi.com/2073-4433/15/12/1541).

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

From the repository root, create the CPU environment and local configuration:

```bash
cd index-xhwi
make install EXTRA=cpu  # or EXTRA=gpu if gpu is available
cp .env.example .env
```

Edit `.env`, then activate the environment:

```bash
source .venv/bin/activate
```

`make install EXTRA=cpu` uses uv to install Python 3.12 and the CPU dependencies in `.venv`. Set `CDSAPI_KEY` in `.env`, or configure `CDSAPI_CONFIG_FILE`, before ERA5 or ERA5-Land runs. An empty `CDSAPI_CONFIG_FILE` disables file fallback.

## Configure

`.env` is the canonical operational configuration and `.env.example` contains every supported field. Every new execution reads `.env` again. Explicit CLI options temporarily override its values for that execution; omitted options preserve them.

All fields already have values in the copied `.env`, but they are required by `Settings`. Review the ERA5 fields below as a practical starting point:

```dotenv
CDSAPI_KEY="replace_with_your_cds_api_key"
CDSAPI_CONFIG_FILE=~/.cdsapirc

SCIENTIFIC_PROFILE=xhwi-2024-v1
XHWI_MINIMUM=0.001
NUMPY_DTYPE=float32
NETCDF_ENGINE=netcdf4
NETCDF_FORMAT=NETCDF4
NETCDF_COMPRESSION=true
NETCDF_COMPLEVEL=4
NETCDF_DTYPE=float32
NETCDF_FILL_VALUE=nan
NETCDF_PROGRESS=true

ERA5_DEVICE=auto
ERA5_TORCH_DTYPE=float32
ERA5_LATITUDE_BLOCK_SIZE=64
ERA5_LONGITUDE_BLOCK_SIZE=64
ERA5_MONTHS_TO_RUN=[1,2,3,4,5,6,7,8,9,10,11,12]
ERA5_CALIBRATION_START=1961-01-01
ERA5_CALIBRATION_END=1990-12-31
ERA5_APPLICATION_START=
ERA5_APPLICATION_END=
ERA5_CALIBRATION_POLICY=create_if_missing
ERA5_PART_EXISTING_POLICY=skip
ERA5_FINAL_EXISTING_POLICY=overwrite
ERA5_CONCAT_INPUT_POLICY=all_matching_parts
ERA5_CALIBRATION_FILE_TEMPLATE=era5/raw_data/xhwi_era5_calib_t2m_max_{start_year}-{end_year}.nc
ERA5_PART_FILE_TEMPLATE=era5/results/monthly/parts/xhwi_{source}_month_{month}.nc
ERA5_FINAL_FILE_TEMPLATE=era5/results/monthly/xhwi_era5_monthly_ind_prod.nc
ERA5_DATASET_ID=ERA5_ARCO
ERA5_SOURCE_ID=era5
ERA5_ZARR_URL=https://arco.datastores.ecmwf.int/cadl-arco-geo-002/arco/reanalysis_era5_single_levels/sfc/geoChunked.zarr
ERA5_ZARR_CHUNKS=auto
ERA5_ZARR_CONSOLIDATED=true
ERA5_REQUEST_TIMEOUT_SECONDS=600
ERA5_VARIABLE_T2M=t2m
ERA5_VARIABLE_T2M_ALIAS=t2m
ERA5_VARIABLE_HUMIDITY=d2m
ERA5_VARIABLE_HUMIDITY_ALIAS=d2m
ERA5_LATITUDE_START=-70.0
ERA5_LATITUDE_END=20.0
ERA5_LONGITUDE_START=-120.0
ERA5_LONGITUDE_END=-5.0
```

Before the first run, review:

- Credentials: set `CDSAPI_KEY`, or confirm `CDSAPI_CONFIG_FILE`; an empty config-file value disables fallback.
- Source URL: verify `ERA5_ZARR_URL`, its chunk mode, consolidated-metadata setting, and request timeout.
- Periods: confirm calibration dates and application bounds. Empty application bounds mean an open interval.
- Months and domain: limit `ERA5_MONTHS_TO_RUN` and the latitude/longitude bounds to the intended run.
- Policies: choose how calibration, existing monthly parts, the final file, and concatenation inputs are handled.
- Paths: verify the calibration, part, and final templates. Relative paths resolve from `index-xhwi`.
- Runtime: select the device, Torch dtype, and spatial block sizes appropriate for the machine.

Calibration policies are `require_existing`, `create_if_missing`, `rebuild`, and `in_memory`. The dedicated calibration command rejects `in_memory`. Part templates contain `{month}` and produce one file per calendar month. Changing `.env` affects the next process; a running process does not reload it. See the **[complete setup and usage guide](docs/getting-started.md)** for every field, including metadata, ERA5-Land, and CMIP6.

## Run

Direct Python commands require the `.venv` activation shown above. Each source provides calibration, monthly processing, concatenation, and a complete workflow:

```bash
python era5/scripts/make_calibration.py
python era5/scripts/run_months.py
python era5/scripts/concat_months.py
python era5/scripts/run_all.py

python era5land/scripts/make_calibration.py
python era5land/scripts/run_months.py
python era5land/scripts/concat_months.py
python era5land/scripts/run_all.py

python cmip6/scripts/make_calibration.py
python cmip6/scripts/run_months.py
python cmip6/scripts/concat_months.py
python cmip6/scripts/run_all.py
```

The equivalent Make targets do not require virtual-environment activation:

```bash
make era5-calibration
make era5-months
make era5-concat
make era5-all

make era5land-calibration
make era5land-months
make era5land-concat
make era5land-all

make cmip6-calibration
make cmip6-months
make cmip6-concat
make cmip6-all
```

For example, overwrite existing ERA5 monthly part files for months 1, 2, and 3:

```bash
python era5/scripts/run_months.py --months-to-run 1 2 3 --part-existing-policy overwrite
make era5-months ARGS="--months-to-run 1 2 3 --part-existing-policy overwrite"
```

See the **[complete setup and usage guide](docs/getting-started.md)** for configuration, command options, file policies, and outputs.
