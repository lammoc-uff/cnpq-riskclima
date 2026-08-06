# Apptainer

The Apptainer image is derived from the CPU Docker runtime image. From `index-spi`, build the Docker image and convert it to SIF:

```bash
docker build --target runtime -t riskclima-spi:local .
apptainer build riskclima-spi.sif docker-daemon://riskclima-spi:local
```

The image contains the installed package and both entry points. It does not contain `.env`, credentials, input data, raw downloads, or results.

## Project configuration

Bind the complete project directory so the entry point reads `/work/.env` and writes data back to the host:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-cmip6
```

Run ERA5 with the same bind and its entry point:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-era5
```

Relative paths in `.env` resolve under `/work`. Ensure every CMIP6 input path is in a bound directory. ERA5 raw data and results use the bound project directories with the default configuration.

## Separate configuration

Bind a configuration file and only the source directories needed by the selected workflow. For CMIP6:

```bash
apptainer exec --cleanenv \
  --bind "/host/config/spi.env:/work/.env:ro,/host/cmip6:/work/cmip6" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-cmip6
```

For ERA5 with a CDS configuration file:

```bash
apptainer exec --cleanenv \
  --bind "/host/config/spi.env:/work/.env:ro,/host/config/cdsapirc:/run/secrets/cdsapirc:ro,/host/era5:/work/era5" \
  --pwd /work \
  riskclima-spi.sif \
  riskclima-spi-era5
```

For that ERA5 command, set the following value in the bound `spi.env`:

```dotenv
CDSAPI_CONFIG_FILE=/run/secrets/cdsapirc
```

Alternatively, set `CDSAPI_KEY` in the configuration. Do not add `.env`, credentials, climate data, generated results, or SIF images to the container build context or Git. These Docker and Apptainer commands were not verified as part of this documentation update.
