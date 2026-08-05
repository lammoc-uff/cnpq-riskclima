# ERA5 XHWI workflow

The ERA5 workflow reads hourly temperature and dewpoint from an authenticated ARCO Zarr store, derives relative humidity, and writes monthly accumulated XHWI as NetCDF.

Run all commands from `index-xhwi` after `make sync` and `.env` configuration.

## Input contract

The configured consolidated Zarr store must provide:

- hourly `t2m` and `d2m` on a common grid
- `time`, `lat`, and `lon` coordinates, or ERA5 names `valid_time`, `latitude`, and `longitude`
- temperature and dewpoint in kelvin with `units` set to `K` or `kelvin`
- coverage for the calibration period and requested application months

Set `CDSAPI_KEY` in `index-xhwi/.env` for remote access. `CDSAPI_CONFIG_FILE=~/.cdsapirc` provides the default fallback; an empty value disables it. The source and variable names are configured by `ERA5_ZARR_URL`, `ERA5_VARIABLE_T2M`, and `ERA5_VARIABLE_HUMIDITY`.

## Preserved scientific defaults

The package retains the notebook defaults:

- calibration period: 1961-01-01 through 1990-12-31
- domain: latitude -70 to 20, longitude -120 to -5
- months: 1 through 12
- temperature threshold: values strictly above 32 degC
- empirical CDF threshold: values above the grid-cell and calendar-month p95
- minimum retained XHWI: values strictly above 0.001
- relative humidity: derived from `t2m` and `d2m`, then clipped to 0-100%
- calibration tasmax: maxima of non-overlapping 24-hour blocks, with an incomplete final block removed
- PyTorch dtype and spatial blocks: float32 and 64 x 64 grid cells

The 32 °C temperature threshold and 95th-percentile CDF threshold are fixed by the methodology. `XHWI_MINIMUM` remains configurable; other source settings use the `ERA5_` fields in `.env.example`.

## Four operations

1. Create the reusable daily-maximum calibration:

   ```bash
   make era5-calibration
   ```

2. Process the configured months into one part file per month:

   ```bash
   make era5-months
   ```

3. Concatenate all matching part files:

   ```bash
   make era5-concat
   ```

4. Run calibration when missing, monthly processing, and concatenation:

   ```bash
   make era5-all
   ```

Pass command options through `ARGS`, for example:

```bash
make era5-months ARGS="--months-to-run 1 2 3 --device cuda --part-existing-policy overwrite"
make era5-all ARGS="--zarr-url https://example/store.zarr --final-existing-policy overwrite"
make era5-concat ARGS="--concat-input-policy all_matching_parts"
```

`.env` is canonical. CLI options shown by `make era5-all ARGS="--help"` override the current invocation only; paths and templates are configured in `.env`.

## Outputs

Defaults are relative to `index-xhwi`:

- calibration: `era5/raw_data/xhwi_era5_calib_t2m_max_1961-1990.nc`
- part files: `era5/results/monthly/parts/xhwi_era5_month_XX.nc`, one per calendar month
- final file: `era5/results/monthly/xhwi_era5_monthly_ind_prod.nc`

The final variable is `xhwi_monthly_accumulated` on `time`, `lat`, and `lon`. Input data and outputs remain outside Git.
