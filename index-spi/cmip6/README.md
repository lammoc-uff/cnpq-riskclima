# CMIP6 SPI workflow

The CMIP6 workflow reads local, preprocessed daily precipitation, converts precipitation flux to monthly accumulation, fits a distribution over a historical calibration period, and writes SPI for one experiment as NetCDF.

Run commands from `index-spi` after `make install` and `.env` configuration. CMIP6 downloading and preprocessing are handled by [`cmip6-downloader`](../../cmip6-downloader/README.md).

## Input contract

`CMIP6_INPUT_FILE` and `CMIP6_CALIBRATION_INPUT_FILE` may point to NetCDF files or Zarr stores. Each dataset must provide:

- the configured precipitation variable, `pr` by default
- the configured time, latitude, and longitude dimensions, `time`, `lat`, and `lon` by default
- daily precipitation flux units of `kg m-2 s-1`, `kg m**-2 s**-1`, or `mm s-1`
- time coverage for its configured application or calibration period
- timestamps compatible with xarray monthly-start resampling
- latitude and longitude coordinates exactly equal between calibration and application data after normalization

For a historical run, both input paths may identify the same dataset. For a scenario run, the calibration input normally remains historical while the experiment input identifies the selected scenario.

Set `CMIP6_MODEL`, `CMIP6_EXPERIMENT`, `CMIP6_MEMBER`, and `CMIP6_GRID` explicitly. These values define output identity and metadata and are not inferred from paths. A historical ensemble mean can use `CMIP6_MEMBER=ensemble_mean`.

## Calculation defaults

The defaults in `.env.example` are:

- SPI scale: 1 month
- distribution and fit: gamma, APP, and `floc=0`
- zero-inflated fitting: enabled
- calibration period: 1961-01-01 through 1990-12-31
- application period: 2015-01-01 through 2050-12-31
- experiment: `ssp245`
- model, member, and grid: `ACCESS-CM2`, `r1i1p1f1`, and `gn`

Daily flux is multiplied by 86400 to obtain daily millimetres, then summed into monthly-start bins. The workflow fits parameters from the calibration input and applies them to the experiment input using [xclim](https://xclim.readthedocs.io/en/stable/indices.html#xclim.indices.standardized_precipitation_index). Set `SPI_SCALE_MONTHS` to calculate SPI-3, SPI-6, SPI-12, or another positive scale.

## Run

Direct Python and uv commands:

```bash
source .venv/bin/activate
python cmip6/scripts/run_spi_cmip6.py

uv run --frozen riskclima-spi-cmip6
```

Equivalent Make command:

```bash
make run_spi_cmip6
```

The entry point has no command-line options. Edit `.env` before starting another experiment.

## Outputs

The default output directory is `cmip6/results`. `CMIP6_OUTPUT_TEMPLATE` builds the `.nc` filename from the SPI scale, model, experiment, member, grid, and application dates, for example:

```text
cmip6/results/spi1_ACCESS-CM2_ssp245_r1i1p1f1_gn_2015-01-01_2050-12-31.nc
```

The output variable is `spi` on `time`, `lat`, and `lon`. An existing file at the exact output path is atomically replaced. Inputs and outputs remain outside Git.

Global metadata records the creator, institution, project, repository, source
model, experiment, member, grid, calibration method, SPI parameters, spatial and temporal coverage,
and the precipitation conversion. The conversion note states that daily `pr`
flux is multiplied by `86400` to obtain `mm day-1` and then summed into monthly
accumulation with `resample(time="MS").sum(min_count=1)`.
