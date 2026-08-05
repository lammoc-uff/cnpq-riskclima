# Complete setup and usage guide

[Back to the root README](../README.md).

Run commands from `index-xhwi`. The package provides ERA5, ERA5-Land, and CMIP6 implementations of monthly accumulated XHWI using xarray and spatial PyTorch blocks.

## Installation

On Windows, use WSL2 with an Ubuntu or Debian distribution. Native Windows shells are not part of the supported workflow.

Install Make and curl for your operating system.

### Ubuntu, Debian, or WSL

```bash
sudo apt update
sudo apt install -y make curl
```

### Fedora

```bash
sudo dnf install -y make curl
```

### Arch Linux

```bash
sudo pacman -S --needed make curl
```

### macOS

Install the Xcode Command Line Tools, which provide a compatible `make`; macOS already provides `curl`:

```bash
xcode-select --install
```

### uv and the project environment

Install [uv](https://docs.astral.sh/uv/):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Open a new shell if `uv` is not yet on `PATH`, then verify both tools:

```bash
make --version
uv --version
```

From the repository root, create the CPU environment and local configuration:

```bash
cd index-xhwi
make install EXTRA=cpu
cp .env.example .env
```

Edit `.env` before running a workflow. `make install EXTRA=cpu` obtains Python 3.12, creates `.venv`, and installs the locked project with CPU dependencies.

Activate `.venv` when running the Python wrappers directly:

```bash
source .venv/bin/activate
```

Activation is optional if you only use Make because Make invokes `uv run`. Use `EXTRA=gpu` consistently to install and run the CUDA wheel.

## Canonical Configuration

`.env` is the canonical source for all operational behavior. `.env.example` is complete and grouped into credentials, scientific profile, NetCDF, metadata, logging, ERA5, ERA5-Land, and CMIP6. Do not commit `.env`.

```dotenv
CDSAPI_KEY="your_key_here"
CDSAPI_CONFIG_FILE=~/.cdsapirc
SCIENTIFIC_PROFILE=xhwi-2024-v1
XHWI_MINIMUM=0.001
ERA5_DEVICE=cpu
ERA5LAND_APPLICATION_START=2010-01-01
ERA5LAND_APPLICATION_END=2025-12-31
CMIP6_SCENARIOS=["historical","ssp245","ssp585"]
```

Every field is already present in the copied `.env`, and each field loaded by a source is required by its `Settings` model. Some canonical entries are intentionally empty. ERA5 and ERA5-Land first use `CDSAPI_KEY`. If it is empty, credentials are read from `CDSAPI_CONFIG_FILE`; an empty `CDSAPI_CONFIG_FILE` disables file fallback. CMIP6 uses prepared Zarr stores. Relative paths resolve from `index-xhwi` when commands are run as documented.

Every command loads `.env`, applies only explicitly provided CLI overrides, and executes. Settings are not cached; edits are visible to the next command or newly created settings instance. A running process does not reload configuration midway through its work.

Scientific formulas, internal coefficients, canonical dimensions, the 32 °C temperature threshold, and the 95th-percentile CDF threshold remain versioned in code. `XHWI_MINIMUM` remains configurable. `SCIENTIFIC_PROFILE` must currently be `xhwi-2024-v1`.

### Credentials and shared settings

The values below are the examples and defaults copied from `.env.example`.

| Variable | Example/default | Purpose | When to change |
| --- | --- | --- | --- |
| `CDSAPI_KEY` | `"replace_with_your_cds_api_key"` | Direct CDS credential for ERA5 and ERA5-Land access. It takes precedence over the config file. | Replace before either reanalysis workflow, or leave empty to use file fallback. |
| `CDSAPI_CONFIG_FILE` | `~/.cdsapirc` | Fallback file containing CDS credentials. An empty value disables fallback. | Change for a nonstandard credential-file location or clear it to disable fallback. |
| `SCIENTIFIC_PROFILE` | `xhwi-2024-v1` | Selects the supported scientific implementation profile. | Do not change unless the implementation adds another accepted profile. |
| `XHWI_MINIMUM` | `0.001` | Shared lower cutoff applied to XHWI values. | Change only for the intended scientific protocol. |
| `NUMPY_DTYPE` | `float32` | In-memory NumPy/xarray floating type; accepted values are `float32` and `float64`. | Use `float64` when precision requirements justify the extra memory. |
| `NETCDF_ENGINE` | `netcdf4` | NetCDF writer engine; accepted values are `netcdf4` and `scipy`. | Change only with a compatible format and compression setup. |
| `NETCDF_FORMAT` | `NETCDF4` | Output container format. | Match the engine: `NETCDF4` or `NETCDF4_CLASSIC` for `netcdf4`; `NETCDF3_64BIT` or `NETCDF3_CLASSIC` for `scipy`. |
| `NETCDF_COMPRESSION` | `true` | Enables NetCDF variable compression. | Disable for uncompressed output; it must be `false` with `scipy`. |
| `NETCDF_COMPLEVEL` | `4` | Compression level from `0` through `9`. | Balance file size against write time. |
| `NETCDF_DTYPE` | `float32` | Floating type stored in NetCDF output. | Use `float64` when output precision requires it. |
| `NETCDF_FILL_VALUE` | `nan` | Fill value encoded for missing floating data. | Change to meet a downstream data convention. |
| `NETCDF_PROGRESS` | `true` | Shows progress while NetCDF output is written. | Disable for quieter batch logs. |

The temperature and CDF thresholds are methodological constants and are not exposed in `.env`. `XHWI_MINIMUM` is the remaining operational scientific input. Fixed formula coefficients are internal and versioned in code. NetCDF validation rejects an engine/format mismatch and rejects compression with `scipy`.

### Metadata and logging

Metadata values are written to output datasets. Change them when ownership, project, licensing, conventions, or provenance differ from the copied example.

| Variable | Example/default | Purpose | When to change |
| --- | --- | --- | --- |
| `METADATA_CREATORS` | `"Marcio Cataldi <mcataldi@id.uff.br>"` | Dataset creator attribution. | Set the responsible creator or creators. |
| `METADATA_INSTITUTION` | `"Climate System Monitoring and Modeling Laboratory (LAMMOC), Universidade Federal Fluminense (UFF), Niteroi, Brazil"` | Producing institution. | Set the institution responsible for the output. |
| `METADATA_PROJECT` | `RiskClima` | Project name stored in metadata. | Change for outputs produced under another project. |
| `METADATA_LICENSE` | `CC-BY-4.0` | Output license identifier. | Change only when the output license differs. |
| `METADATA_REFERENCES` | `https://riskclima.com.br/` | Project or dataset reference URL. | Point to the applicable reference. |
| `METADATA_REPOSITORY` | `https://github.com/lammoc-uff/cnpq-riskclima` | Source repository URL. | Change for a maintained fork or another source location. |
| `METADATA_CONVENTIONS` | `CF-1.10` | Metadata conventions declaration. | Change only when output follows another convention version. |
| `METADATA_PROCESSING_LEVEL` | `"Processed data"` | Human-readable processing-level label. | Adjust to the output classification in use. |
| `LOG_LEVEL` | `INFO` | Minimum log severity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. | Use `DEBUG` for diagnosis or a higher level for quieter operation. |
| `LOG_FORMAT` | `"%(levelname)s %(name)s: %(message)s"` | Python logging record format. | Change to match local log collection requirements. |

### ERA5 catalog

| Variable | Example/default | Purpose | When to change |
| --- | --- | --- | --- |
| `ERA5_DEVICE` | `auto` | PyTorch device. `auto` selects CUDA when available, otherwise CPU; `cpu` and `cuda` are explicit. | Pin the execution device or use `cuda` to require an available GPU. |
| `ERA5_TORCH_DTYPE` | `float32` | PyTorch compute type, `float32` or `float64`. | Use `float64` for higher precision at greater memory and compute cost. |
| `ERA5_LATITUDE_BLOCK_SIZE`, `ERA5_LONGITUDE_BLOCK_SIZE` | `64`, `64` | Number of grid cells processed per spatial PyTorch block. Both must be positive. | Reduce for limited device memory; increase after measuring available memory and throughput. |
| `ERA5_MONTHS_TO_RUN` | `[1,2,3,4,5,6,7,8,9,10,11,12]` | Unique calendar months to process, each from `1` through `12`. | Restrict seasonal or incremental runs. |
| `ERA5_CALIBRATION_START`, `ERA5_CALIBRATION_END` | `1961-01-01`, `1990-12-31` | Inclusive calibration bounds. They also supply `{start_year}` and `{end_year}` in paths. | Set the calibration baseline required by the analysis. |
| `ERA5_APPLICATION_START`, `ERA5_APPLICATION_END` | empty, empty | Optional application bounds. Empty values mean an open interval on that side. | Bound processing to a specific period. |
| `ERA5_CALIBRATION_POLICY` | `create_if_missing` | Controls calibration acquisition. | Choose `require_existing`, `create_if_missing`, `rebuild`, or `in_memory`. |
| `ERA5_PART_EXISTING_POLICY` | `skip` | Handles an existing monthly part. | Choose `skip`, `overwrite`, or `fail`. |
| `ERA5_FINAL_EXISTING_POLICY` | `overwrite` | Handles an existing final output. | Choose `skip`, `overwrite`, or `fail`. |
| `ERA5_CONCAT_INPUT_POLICY` | `all_matching_parts` | Chooses parts for final concatenation. | Use `current_run` only with `run-all`; standalone concat requires `all_matching_parts`. |
| `ERA5_CALIBRATION_FILE_TEMPLATE` | `era5/raw_data/xhwi_era5_calib_t2m_max_{start_year}-{end_year}.nc` | Calibration output path; `{start_year}` and `{end_year}` are required. | Change the output layout while retaining both placeholders. |
| `ERA5_PART_FILE_TEMPLATE` | `era5/results/monthly/parts/xhwi_{source}_month_{month}.nc` | Monthly-part path; `{month}` is required and `{source}` renders from the source ID without hyphens. | Change the parts layout while retaining `{month}`. |
| `ERA5_FINAL_FILE_TEMPLATE` | `era5/results/monthly/xhwi_era5_monthly_ind_prod.nc` | Final concatenated output path. | Change the final destination or filename. |
| `ERA5_DATASET_ID` | `ERA5_ARCO` | Dataset identity written to output metadata. | Change when the configured input dataset identity changes. |
| `ERA5_SOURCE_ID` | `era5` | Safe source identity used in metadata and `{source}` paths. | Change only when a distinct source identity is needed; path separators and glob characters are invalid. |
| `ERA5_ZARR_URL` | `https://arco.datastores.ecmwf.int/cadl-arco-geo-002/arco/reanalysis_era5_single_levels/sfc/geoChunked.zarr` | ERA5 ARCO Zarr source. | Verify before the first run and update if the store location changes. |
| `ERA5_ZARR_CHUNKS` | `auto` | Chunk selection passed when the Zarr store is opened. | Change for a tested source-specific chunk strategy. |
| `ERA5_ZARR_CONSOLIDATED` | `true` | Tells the reader to use consolidated Zarr metadata. | Match the metadata layout of another store. |
| `ERA5_REQUEST_TIMEOUT_SECONDS` | `600` | Positive remote request timeout in seconds. | Adjust for network and store response times. |
| `ERA5_VARIABLE_T2M`, `ERA5_VARIABLE_T2M_ALIAS` | `t2m`, `t2m` | Preferred and fallback source names for 2 m temperature. | Change either lookup name when another store uses a different schema. |
| `ERA5_VARIABLE_HUMIDITY`, `ERA5_VARIABLE_HUMIDITY_ALIAS` | `d2m`, `d2m` | Preferred and fallback source names for the dewpoint input used to derive humidity. | Change either lookup name when another store uses a different schema. |
| `ERA5_LATITUDE_START`, `ERA5_LATITUDE_END` | `-70.0`, `20.0` | Latitude slice bounds in source coordinate order. | Set the target domain and preserve the store's coordinate ordering. |
| `ERA5_LONGITUDE_START`, `ERA5_LONGITUDE_END` | `-120.0`, `-5.0` | Longitude slice bounds in source coordinates. | Set the target domain using the store's longitude convention. |

Before an ERA5 run, verify credentials, `ERA5_ZARR_URL`, periods, months, domain, policies, output templates, device, dtype, and block sizes.

### ERA5-Land catalog

ERA5-Land uses separate temperature and dewpoint stores. Its copied application period is bounded to 2010-2025, unlike ERA5's open application interval, and its default domain and month subset are narrower.

| Variable | Example/default | Purpose | When to change |
| --- | --- | --- | --- |
| `ERA5LAND_DEVICE` | `auto` | PyTorch device with the same `auto`, `cpu`, and `cuda` behavior as ERA5. | Pin the device or require CUDA. |
| `ERA5LAND_TORCH_DTYPE` | `float32` | PyTorch compute type, `float32` or `float64`. | Change for precision requirements and available memory. |
| `ERA5LAND_LATITUDE_BLOCK_SIZE`, `ERA5LAND_LONGITUDE_BLOCK_SIZE` | `64`, `64` | Positive spatial block dimensions. | Tune to device memory and measured throughput. |
| `ERA5LAND_MONTHS_TO_RUN` | `[1,2,3,9,10,11,12]` | Unique calendar months selected for processing. | Change the seasonal or incremental selection. |
| `ERA5LAND_CALIBRATION_START`, `ERA5LAND_CALIBRATION_END` | `1961-01-01`, `1990-12-31` | Inclusive calibration bounds and years used by the calibration path. | Set the required baseline. |
| `ERA5LAND_APPLICATION_START`, `ERA5LAND_APPLICATION_END` | `2010-01-01`, `2025-12-31` | Application bounds. Either value may be empty for an open interval on that side. | Change the ERA5-Land analysis period. |
| `ERA5LAND_CALIBRATION_POLICY` | `create_if_missing` | Controls calibration acquisition. | Choose `require_existing`, `create_if_missing`, `rebuild`, or `in_memory`. |
| `ERA5LAND_PART_EXISTING_POLICY` | `skip` | Handles existing monthly parts. | Choose `skip`, `overwrite`, or `fail`. |
| `ERA5LAND_FINAL_EXISTING_POLICY` | `overwrite` | Handles an existing final output. | Choose `skip`, `overwrite`, or `fail`. |
| `ERA5LAND_CONCAT_INPUT_POLICY` | `all_matching_parts` | Chooses concatenation inputs. | Use `current_run` only with `run-all`. |
| `ERA5LAND_CALIBRATION_FILE_TEMPLATE` | `era5land/raw_data/xhwi_era5land_calib_t2m_max_{start_year}-{end_year}.nc` | Calibration output; `{start_year}` and `{end_year}` are required. | Change the layout while retaining both placeholders. |
| `ERA5LAND_PART_FILE_TEMPLATE` | `era5land/results/monthly/parts/xhwi_{source}_month_{month}.nc` | Monthly-part output; `{month}` is required. | Change the parts layout while retaining `{month}`. |
| `ERA5LAND_FINAL_FILE_TEMPLATE` | `era5land/results/monthly/xhwi_era5land_monthly_ind_prod.nc` | Final output path. | Change the final destination or filename. |
| `ERA5LAND_DATASET_ID` | `ERA5_LAND_ARCO` | Dataset identity written to metadata. | Change when the input dataset identity changes. |
| `ERA5LAND_SOURCE_ID` | `era5-land` | Safe source identity; `{source}` renders it as `era5land`. | Change only for a distinct source identity. |
| `ERA5LAND_ZARR_URL` | `https://arco.datastores.ecmwf.int/cadl-arco-geo-007/arco/reanalysis_era5_land/sfc-2m-temperature/geoChunked.zarr` | ERA5-Land 2 m temperature store. | Verify before the first run or replace for another store. |
| `ERA5LAND_DEWPOINT_ZARR_URL` | `https://arco.datastores.ecmwf.int/cadl-arco-geo-007/arco/reanalysis_era5_land/sfc-2m-dewpoint-temperature/geoChunked.zarr` | Separate ERA5-Land 2 m dewpoint store. | Verify with the temperature store and update when its location changes. |
| `ERA5LAND_ZARR_CHUNKS` | `auto` | Chunk selection used to open both stores. | Change for a tested chunk strategy. |
| `ERA5LAND_ZARR_CONSOLIDATED` | `true` | Uses consolidated metadata for the Zarr stores. | Match replacement stores. |
| `ERA5LAND_REQUEST_TIMEOUT_SECONDS` | `600` | Positive remote request timeout in seconds. | Adjust for network and store response times. |
| `ERA5LAND_VARIABLE_T2M`, `ERA5LAND_VARIABLE_T2M_ALIAS` | `2m_temperature`, `t2m` | Preferred and fallback source names for temperature. | Change either lookup name for another store schema. |
| `ERA5LAND_VARIABLE_HUMIDITY`, `ERA5LAND_VARIABLE_HUMIDITY_ALIAS` | `2m_dewpoint_temperature`, `d2m` | Preferred and fallback source names for dewpoint. | Change either lookup name for another store schema. |
| `ERA5LAND_LATITUDE_START`, `ERA5LAND_LATITUDE_END` | `-24.0`, `-20.0` | Latitude slice bounds in source coordinate order. | Set the target domain while preserving coordinate order. |
| `ERA5LAND_LONGITUDE_START`, `ERA5LAND_LONGITUDE_END` | `-46.0`, `-40.0` | Longitude slice bounds in source coordinates. | Set the domain using the source longitude convention. |

Before an ERA5-Land run, verify credentials, both Zarr URLs, periods, selected months, domain, policies, paths, device, dtype, block sizes, aliases, and timeout.

### CMIP6 catalog

| Variable | Example/default | Purpose | When to change |
| --- | --- | --- | --- |
| `CMIP6_DEVICE` | `auto` | PyTorch device with `auto`, `cpu`, or `cuda`. | Pin the device or require CUDA. |
| `CMIP6_TORCH_DTYPE` | `float32` | PyTorch compute type, `float32` or `float64`. | Change for precision requirements and available memory. |
| `CMIP6_LATITUDE_BLOCK_SIZE`, `CMIP6_LONGITUDE_BLOCK_SIZE` | `16`, `16` | Positive spatial PyTorch block dimensions. | Tune for the model grid and device memory. |
| `CMIP6_MONTHS_TO_RUN` | `[1,2,3,4,5,6,7,8,9,10,11,12]` | Unique calendar months to process. | Restrict seasonal or incremental runs. |
| `CMIP6_CALIBRATION_START`, `CMIP6_CALIBRATION_END` | `1961-01-01`, `1990-12-31` | Inclusive calibration bounds and years rendered in calibration output. | Match the calibration source and protocol. |
| `CMIP6_APPLICATION_START`, `CMIP6_APPLICATION_END` | empty, empty | Optional application bounds. Empty values mean an open interval on that side. | Bound scenario processing to a target period. |
| `CMIP6_CALIBRATION_POLICY` | `create_if_missing` | Controls calibration acquisition. | Choose `require_existing`, `create_if_missing`, `rebuild`, or `in_memory`. |
| `CMIP6_PART_EXISTING_POLICY` | `skip` | Handles existing monthly parts. | Choose `skip`, `overwrite`, or `fail`. |
| `CMIP6_FINAL_EXISTING_POLICY` | `overwrite` | Handles existing final outputs. | Choose `skip`, `overwrite`, or `fail`. |
| `CMIP6_CONCAT_INPUT_POLICY` | `all_matching_parts` | Chooses concatenation inputs. | Use `current_run` only with `run-all`. |
| `CMIP6_CALIBRATION_FILE_TEMPLATE` | `cmip6/{model}/results/xhwi_torch/xhwi_cmip6_{model}_historical_tasmax_{start_year}-{end_year}.nc` | Calibration output. `{model}`, `{start_year}`, and `{end_year}` are required. | Change the layout while retaining required placeholders. |
| `CMIP6_PART_FILE_TEMPLATE` | `cmip6/{model}/results/xhwi_torch/{scenario}/parts/xhwi_cmip6_{model}_{scenario}_{member}_month_{month}.nc` | Monthly part. `{model}`, `{scenario}`, `{member}`, and `{month}` are required. | Change the layout while retaining required placeholders. |
| `CMIP6_FINAL_FILE_TEMPLATE` | `cmip6/{model}/results/xhwi_torch/xhwi_cmip6_{model}_{scenario}_{member}_monthly_accumulated_torch.nc` | Scenario final output. `{model}`, `{scenario}`, and `{member}` are required. | Change the destination while retaining required identity placeholders. |
| `CMIP6_DATASET_ID` | `CMIP6` | Dataset identity written to metadata. | Change for a different input collection identity. |
| `CMIP6_SOURCE_ID` | `cmip6` | Safe source identity used in metadata and optional paths. | Change only for a distinct source identity. |
| `CMIP6_MODEL` | `BCC-CSM2-MR` | Model identity used to render input and output paths. | Set the model being processed. |
| `CMIP6_MEMBER` | `r1i1p1f1` | Ensemble member identity used in paths. | Set the prepared member being processed. |
| `CMIP6_GRID` | `gn` | Grid label used to locate prepared inputs. | Match the model stores. |
| `CMIP6_SCENARIOS` | `["historical","ssp245","ssp585"]` | Unique allowed scenario identities. | List every scenario intended for processing. |
| `CMIP6_DEFAULT_SCENARIO` | `historical` | Scenario used by single-scenario operations; it must appear in `CMIP6_SCENARIOS`. | Select the usual scenario for direct commands. |
| `CMIP6_TIME_CHUNK` | `744` | Time chunk for scenario input processing. | Tune for memory, store layout, and task size. |
| `CMIP6_CALIBRATION_TIME_CHUNK` | `-1` | Time chunk for calibration input; `-1` requests the full time dimension as one chunk. | Use a finite chunk when calibration memory use requires it. |
| `CMIP6_SPATIAL_CHUNK` | `32` | Spatial chunk size used when opening CMIP6 stores. | Tune for the source layout and memory. |
| `CMIP6_ZARR_CONSOLIDATED` | `true` | Uses consolidated metadata for prepared Zarr stores. | Match the metadata layout of replacement stores. |
| `CMIP6_VARIABLE_TAS` | `tas` | Near-surface air-temperature variable in scenario stores. | Match the prepared store schema. |
| `CMIP6_VARIABLE_HUSS` | `huss` | Near-surface specific-humidity variable in scenario stores. | Match the prepared store schema. |
| `CMIP6_VARIABLE_TASMAX` | `tasmax` | Daily maximum temperature variable in the calibration store. | Match the calibration store schema. |
| `CMIP6_STANDARD_PRESSURE_PA` | `101325.0` | Fixed pressure in pascals used to derive humidity quantities. | Change only when the processing protocol specifies another pressure. |
| `CMIP6_INTERPOLATION_FREQUENCY` | `1h` | Target temporal frequency for scenario interpolation. | Match the required output cadence. |
| `CMIP6_INTERPOLATION_METHOD` | `linear` | xarray interpolation method, `linear` or `nearest`. | Select the method required by the protocol and source cadence. |
| `CMIP6_SCENARIO_TAS_TEMPLATE` | `cmip6/{model}/{scenario}/3hr/tas/{grid}/member-{member}.zarr` | Scenario temperature input. `{model}`, `{scenario}`, `{member}`, and `{grid}` are required. | Point to the prepared temperature layout while retaining all placeholders. |
| `CMIP6_SCENARIO_HUSS_TEMPLATE` | `cmip6/{model}/{scenario}/3hr/huss/{grid}/member-{member}.zarr` | Scenario humidity input. `{model}`, `{scenario}`, `{member}`, and `{grid}` are required. | Point to the prepared humidity layout while retaining all placeholders. |
| `CMIP6_CALIBRATION_SOURCE_TEMPLATE` | `cmip6/{model}/historical/day/tasmax/{grid}/ensemble_mean.zarr` | Daily maximum-temperature calibration input. `{model}` and `{grid}` are required. | Point to the prepared calibration store while retaining both placeholders. |

Before a CMIP6 run, verify model, member, grid, scenarios and default, all three input templates, calibration and application periods, months, policies, output templates, chunks, variable names, pressure, interpolation, device, dtype, and block sizes.

### Periods

Calibration and application periods are configured independently for every source. Empty `APPLICATION_START` or `APPLICATION_END` values mean that side of the interval is open:

```dotenv
ERA5_APPLICATION_START=
ERA5_APPLICATION_END=
CMIP6_APPLICATION_START=2041-01-01
CMIP6_APPLICATION_END=2070-12-31
```

### Templates

Calibration, part, final, and CMIP6 input layout paths are templates. Part templates require `{month}` and each monthly command writes one file per requested month. Calibration templates require `{start_year}` and `{end_year}`. CMIP6 output templates also require model identity fields, and CMIP6 input templates require the identity fields needed to locate their stores. Unknown placeholders fail settings validation.

The accepted placeholders are `{start_year}`, `{end_year}`, `{model}`, `{member}`, `{grid}`, `{scenario}`, `{month}`, and `{source}`. Relative rendered paths resolve from `index-xhwi` under the documented invocation. Source IDs, CMIP6 model, member, grid, and scenario values must be safe path segments.

For example:

```dotenv
ERA5_CALIBRATION_START=1981-01-01
ERA5_CALIBRATION_END=2010-12-31
ERA5_CALIBRATION_FILE_TEMPLATE=era5/raw_data/xhwi_era5_calib_t2m_max_{start_year}-{end_year}.nc
```

This renders `era5/raw_data/xhwi_era5_calib_t2m_max_1981-2010.nc`. The defaults retain the existing 1961-1990 names.

### Policies

Each source has one calibration policy:

| Policy | Behavior |
| --- | --- |
| `require_existing` | Validate and use the configured file; fail if missing. |
| `create_if_missing` | Validate an existing file or create a missing one. |
| `rebuild` | Recompute and replace the configured file. |
| `in_memory` | Compute calibration from source data for the run without requiring or writing a file. |

Parts and final outputs independently use `skip`, `overwrite`, or `fail`. Calibration does not use these policies. `CONCAT_INPUT_POLICY=all_matching_parts` includes prior monthly parts and supports overlapping incremental runs without duplicate files. `current_run` includes only paths returned by the current `run-all`; standalone concat rejects it. The dedicated `make-calibration` command rejects `in_memory` because it must produce a file.

## Commands

### Direct Python commands

Direct Python commands require an active `.venv`:

```bash
source .venv/bin/activate

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

### Make targets

Make exposes the same 12 commands and does not require virtual-environment activation:

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

CLI options override the corresponding `.env` fields for one execution. For example:

```bash
python era5/scripts/run_months.py --months-to-run 1 2 3 --part-existing-policy overwrite
make era5-months ARGS="--months-to-run 1 2 3 --part-existing-policy overwrite"
make era5-all ARGS="--calibration-policy rebuild --final-existing-policy fail"
make era5land-months ARGS="--zarr-url https://example.invalid/store.zarr"
make cmip6-months ARGS="--default-scenario ssp245 --model BCC-CSM2-MR"
make cmip6-all ARGS="--scenarios historical ssp245 --part-existing-policy overwrite"
```

Use the corresponding wrapper or Make target to inspect command-specific overrides:

```bash
python era5/scripts/run_all.py --help
make era5-all ARGS="--help"
```

Paths, templates, NetCDF settings, and most source details are configured only in `.env`; the CLI does not duplicate every field.

## Inputs and Outputs

Source-specific contracts remain in [ERA5](../era5/README.md), [ERA5-Land](../era5land/README.md), and [CMIP6](../cmip6/README.md). The operational paths and variable names in `.env` take precedence over examples in those files.

NetCDF engine, format, compression, compression level, storage dtype, fill value, and progress display are shared settings. `netcdf4` supports `NETCDF4` and `NETCDF4_CLASSIC` with optional compression. `scipy` supports the configured NetCDF3 formats and requires compression to be disabled. `NUMPY_DTYPE` controls loaded arrays, `NETCDF_DTYPE` controls storage, and each source has a separate `TORCH_DTYPE`.

Shared metadata fields and source `DATASET_ID`/`SOURCE_ID` values are written to final datasets. Scientific descriptions are generated by the versioned implementation.

## Validation

```bash
make format EXTRA=cpu
make check EXTRA=cpu
uv lock --check
git diff --check
```

Tests use synthetic local arrays and do not access climate stores. Production validation still requires representative ERA5, ERA5-Land, and CMIP6 data, especially non-Gregorian CMIP6 calendars and remote-store authentication.

## Containers and Notebooks

Build CPU or GPU Docker images with `make docker-cpu` or `make docker-gpu`. See [Apptainer operations](apptainer.md) for SIF usage.

The notebooks and their READMEs are self-contained and do not use package `.env` settings. They are intentionally preserved separately from this operational refactoring.
