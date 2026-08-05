# CMIP6 XHWI workflow

The CMIP6 workflow reads prepared local Zarr stores for one model, stages a historical daily-maximum calibration, derives hourly relative humidity, and writes monthly accumulated XHWI for historical, SSP2-4.5, and SSP5-8.5 experiments.

Run all commands from `index-xhwi` after `make sync` and `.env` configuration.

## Input contract

With the default model `BCC-CSM2-MR`, member `r1i1p1f1`, grid `gn`, and root `cmip6`, the required stores are:

```text
cmip6/BCC-CSM2-MR/historical/day/tasmax/gn/ensemble_mean.zarr
cmip6/BCC-CSM2-MR/<scenario>/3hr/tas/gn/member-r1i1p1f1.zarr
cmip6/BCC-CSM2-MR/<scenario>/3hr/huss/gn/member-r1i1p1f1.zarr
```

`<scenario>` is `historical`, `ssp245`, or `ssp585`. Each store must use `time`, `lat`, and `lon`; auxiliary variables are removed. `tas` and `tasmax` must be temperature fields with kelvin units identified as `K` or `kelvin`, and `huss` must be specific humidity as a mass fraction. The scenario fields and historical `tasmax` calibration must share a compatible spatial grid.

Calibration always uses the historical daily `tasmax` `ensemble_mean.zarr`, not the configured member store. Scenario processing uses that calibration with the selected member's 3-hourly `tas` and `huss`.

## Preserved scientific defaults

The package retains the notebook defaults:

- calibration period: 1961-01-01 through 1990-12-31
- model, member, and grid: `BCC-CSM2-MR`, `r1i1p1f1`, and `gn`
- scenarios: `historical`, `ssp245`, and `ssp585`
- months: 1 through 12
- methodological temperature threshold: values strictly above 32 degC (fixed)
- methodological empirical CDF threshold: values above the grid-cell and calendar-month p95 (fixed)
- minimum retained XHWI: values strictly above the shared `XHWI_MINIMUM` environment setting (0.001 by default)
- temporal conversion: sort 3-hourly `tas` and `huss`, then linearly interpolate each to 1-hour resolution before relative humidity is calculated
- relative humidity: derived from `huss` and `tas` with Bolton saturation vapor pressure and a constant pressure of 101325 Pa, then clipped to 0-100%
- PyTorch dtype and spatial blocks: float32 and 16 x 16 grid cells
- input chunks: 744 hourly time steps and 32 x 32 spatial cells

The resampling and month-selection paths require explicit validation for non-Gregorian CMIP6 calendars. Do not assume equivalent timestamps, month boundaries, or interpolation behavior without an end-to-end check against the source calendar.

Override operational defaults with `CMIP6_` variables from `.env.example`. The fixed 32 degC and p95 methodological thresholds are not overrides; `XHWI_MINIMUM` is a shared environment setting. CLI supports temporary `--model`, `--member`, and `--grid` overrides; input and output templates remain in `.env`.

## Four operations

1. Stage the historical `tasmax` ensemble-mean calibration:

   ```bash
   make cmip6-calibration
   ```

2. Process selected months into one part per month for one scenario, historical by default:

   ```bash
   make cmip6-months
   ```

3. Concatenate matching part files for one scenario, historical by default:

   ```bash
   make cmip6-concat
   ```

4. Run calibration when missing, then process and concatenate every configured scenario:

   ```bash
   make cmip6-all
   ```

Pass options through `ARGS`:

```bash
make cmip6-months ARGS="--default-scenario ssp245 --months-to-run 1 2 3 --device cuda --part-existing-policy overwrite"
make cmip6-concat ARGS="--default-scenario ssp245 --concat-input-policy all_matching_parts"
make cmip6-all ARGS="--scenarios historical ssp585 --months-to-run 1 7 --final-existing-policy overwrite"
```

`.env` is canonical. CLI options shown by `make cmip6-all ARGS="--help"` override only the current invocation. Templates include model, scenario, member, grid, and month identity as required by their role.

## Outputs

Defaults are relative to `index-xhwi`:

- staged calibration: `cmip6/BCC-CSM2-MR/results/xhwi_torch/xhwi_cmip6_BCC-CSM2-MR_historical_tasmax_1961-1990.nc`
- part files: `cmip6/BCC-CSM2-MR/results/xhwi_torch/<scenario>/parts/xhwi_cmip6_BCC-CSM2-MR_<scenario>_r1i1p1f1_month_XX.nc`
- final files: `cmip6/BCC-CSM2-MR/results/xhwi_torch/xhwi_cmip6_BCC-CSM2-MR_<scenario>_r1i1p1f1_monthly_accumulated_torch.nc`

The final variable is `xhwi_monthly_accumulated` on `time`, `lat`, and `lon`. Inputs and outputs remain outside Git.
