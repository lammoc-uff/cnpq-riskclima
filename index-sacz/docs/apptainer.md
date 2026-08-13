# Apptainer

Build the Docker runtime image from `index-sacz` and convert it to SIF:

```bash
docker build --target runtime -t riskclima-sacz:local .
apptainer build riskclima-sacz.sif docker-daemon://riskclima-sacz:local
```

The image contains Python, the package dependencies, CDO, and the Google Cloud CLI. It does not contain `.env`, climate data, coefficients, the area shapefile, or generated results.

Bind the complete project directory so relative paths resolve from `/work`:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-sacz.sif \
  riskclima-sacz-process-era5
```

Use the other installed commands for the remaining stages:

```text
riskclima-sacz-index-era5
riskclima-sacz-process-cmip6
riskclima-sacz-index-cmip6
```

The project bind must contain the configured `areas`, `coefs`, ERA5 directories, and CMIP6 directories. The ERA5 workflow still depends on access to the public ARCO ERA5 bucket through `gsutil`.
