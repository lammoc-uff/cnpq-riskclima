# ERA5 SPI workflow

The ERA5 workflow downloads monthly total precipitation from the Copernicus Climate Data Store (CDS) on every run, converts mean daily precipitation to monthly accumulation, and writes SPI as NetCDF.

Run commands from `index-spi` after `make install`, `.env` configuration, and acceptance of the dataset terms in CDS.

## Acquisition contract

The default source is `reanalysis-era5-single-levels-monthly-means`, product type `monthly_averaged_reanalysis`, variable `total_precipitation`, and time `00:00`. The request area is sent to CDS as north, west, south, east from the configured latitude and longitude bounds.

With the default period, 1940-01-01 through 2026-07-01, CDS receives two requests: one for every month in the complete years 1940-2025 and one for January-July 2026. More generally, an end month before December splits earlier complete years from the partial final year. A period ending in December uses one complete-period request.

Each response is downloaded beside the final file as `.complete-years.part.nc` or `.current-year.part.nc`. The parts are normalized, checked for exactly equal latitude and longitude coordinates, concatenated with exact coordinate alignment, validated for exact monthly coverage, and written through a temporary `.nc.tmp` file. The completed temporary file atomically replaces the exact path produced by `ERA5_RAW_FILE_TEMPLATE`. All `.part.nc` and `.nc.tmp` files are removed after success or failure; an existing final `.nc` remains intact if acquisition or validation fails.

Final raw `.nc` files for other date-based paths are retained. Repeated runs using the same configured path overwrite that path only after successful validation.

`CDSAPI_URL` always selects the endpoint. If `CDSAPI_KEY` is nonblank, it is used and `CDSAPI_CONFIG_FILE` is not read. If the environment key is empty, the workflow reads only `key` from `CDSAPI_CONFIG_FILE`, `~/.cdsapirc` by default. A `url` entry may remain in the file but does not override `CDSAPI_URL`:

```yaml
url: https://cds.climate.copernicus.eu/api
key: your-personal-access-token
```

## Input normalization

Downloaded data must contain the configured `tp`, time, latitude, and longitude names. The defaults are `tp`, `valid_time`, `latitude`, and `longitude`. The workflow renames the dimensions to `time`, `lat`, and `lon` and sorts all three coordinates increasingly.

ERA5 monthly averaged total precipitation is a mean daily rate in metres. For each timestamp, monthly precipitation is calculated as:

```text
tp * 1000 * days_in_month
```

The result is monthly accumulated precipitation in `mm month-1`. Accepted source unit attributes are `m`, `m/day`, `m day-1`, and `m of water equivalent`. The same monthly data supplies the configured 1961-1990 calibration and the application period.

## Configure

The ERA5 fields in `.env.example` control credentials, dataset request, domain, source names, periods, raw filename, and output filename. Important constraints are:

- `ERA5_DOWNLOAD_START` must be January 1.
- `ERA5_DOWNLOAD_END` must be the first day of a month and is included in the request.
- The download must cover the calibration start and application end.
- Minimum latitude and longitude must be below their corresponding maximum values.
- `ERA5_RAW_FILE_TEMPLATE` must support `{start}` and `{end}` if those values are used in the filename.
- `ERA5_SPATIAL_CHUNK` controls the latitude and longitude Dask chunk size; `.env.example` uses `32`.
- `ERA5_DASK_WORKERS` limits concurrent threaded Dask tasks; `.env.example` uses `1`.

## Run

Direct Python and uv commands:

```bash
source .venv/bin/activate
python era5/scripts/run_spi_era5.py

uv run --frozen riskclima-spi-era5
```

Equivalent Make command:

```bash
make run_spi_era5
```

The entry point has no command-line options. Edit `.env` before changing the acquisition or SPI calculation. SPI processing opens the newly downloaded normalized raw file lazily with the complete time axis in each chunk and bounded spatial chunks. The final NetCDF write remains lazy and runs with the configured worker limit, preventing all spatial cells from being calculated in memory at once. The chunk and worker settings apply to SPI computation, not the physical raw NetCDF encoding.

## Outputs

Default paths are relative to `index-spi`:

- retained raw input: `era5/raw_data/era5_tp_monthly_1940-01-01_2026-07-01.nc`
- SPI output: `era5/results/spi1_era5_1940-01-01_2026-07-01.nc`

The output variable is `spi` on `time`, `lat`, and `lon`. An existing SPI file at the exact output path is atomically replaced. Raw data and outputs remain outside Git.

Global metadata records the creator, institution, project, repository, ERA5
dataset and product type, calibration method, SPI parameters, spatial and
temporal coverage, and the precipitation conversion. The conversion note
explains that monthly averaged `tp` has an effective one-day processing period
and is multiplied by `1000` and `days_in_month` to obtain monthly accumulation
in `mm month-1`.

## References

- [ERA5 monthly averaged data on single levels from 1940 to present](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview)
- [ERA5 data documentation](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation)
- [ERA5 family data documentation: mean rates and accumulations](https://confluence.ecmwf.int/pages/viewpage.action?pageId=197702790)
- [ECMWF parameter database: total precipitation](https://codes.ecmwf.int/grib/param-db/228)
