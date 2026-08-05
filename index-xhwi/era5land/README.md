# ERA5-Land XHWI workflow

The ERA5-Land workflow reads hourly temperature and dewpoint from authenticated ARCO Zarr stores, derives relative humidity, limits data to the configured application period, and writes monthly accumulated XHWI as NetCDF.

Run all commands from `index-xhwi` after `make sync` and `.env` configuration.

## Input contract

Two Zarr stores must provide:

- hourly `2m_temperature` and `2m_dewpoint_temperature` on exactly matching grids and timestamps
- `time`, `lat`, and `lon` coordinates, or ERA5 names `valid_time`, `latitude`, and `longitude`
- both variables in kelvin with `units` set to `K` or `kelvin`
- coverage for the 1961-1990 calibration and configured application period

Set `CDSAPI_KEY` in `index-xhwi/.env` for remote access. `CDSAPI_CONFIG_FILE=~/.cdsapirc` provides the default fallback; an empty value disables it. Configure the stores with `ERA5LAND_ZARR_URL` and `ERA5LAND_DEWPOINT_ZARR_URL`; `--zarr-url` temporarily overrides only the temperature store.

## Preserved scientific defaults

The package retains the notebook defaults:

- calibration period: 1961-01-01 through 1990-12-31
- application period: 2010-01-01 through 2025-12-31
- domain: latitude -24 to -20, longitude -46 to -40
- months: 1, 2, 3, 9, 10, 11, and 12
- temperature threshold: values strictly above 32 degC
- empirical CDF threshold: values above the grid-cell and calendar-month p95
- minimum retained XHWI: values strictly above 0.001
- relative humidity: derived from temperature and dewpoint, then clipped to 0-100%
- calibration tasmax: maxima of non-overlapping 24-hour blocks, with an incomplete final block removed
- PyTorch dtype and spatial blocks: float32 and 64 x 64 grid cells

The 32 °C temperature threshold and 95th-percentile CDF threshold are fixed by the methodology. `XHWI_MINIMUM` remains configurable; other settings use the `ERA5LAND_` fields listed in `.env.example`.

## Four operations

1. Create the reusable daily-maximum calibration:

   ```bash
   make era5land-calibration
   ```

2. Process the configured months into one part file per month:

   ```bash
   make era5land-months
   ```

3. Concatenate all matching part files:

   ```bash
   make era5land-concat
   ```

4. Run calibration when missing, monthly processing, and concatenation:

   ```bash
   make era5land-all
   ```

Pass options through `ARGS`:

```bash
make era5land-months ARGS="--months-to-run 1 2 3 --device cuda --part-existing-policy overwrite"
make era5land-all ARGS="--zarr-url https://example/store.zarr --final-existing-policy overwrite"
make era5land-concat ARGS="--concat-input-policy all_matching_parts"
```

`.env` is canonical. CLI options shown by `make era5land-all ARGS="--help"` override the current invocation only; paths and templates are configured in `.env`.

## Outputs

Defaults are relative to `index-xhwi`:

- calibration: `era5land/raw_data/xhwi_era5land_calib_t2m_max_1961-1990.nc`
- part files: `era5land/results/monthly/parts/xhwi_era5land_month_XX.nc`, one per calendar month
- final file: `era5land/results/monthly/xhwi_era5land_monthly_ind_prod.nc`

The final variable is `xhwi_monthly_accumulated` on `time`, `lat`, and `lon`. Input data and outputs remain outside Git.
