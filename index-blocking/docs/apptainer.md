# Apptainer

Build the Docker runtime image from `index-blocking` and convert it to SIF:

```bash
docker build --target runtime -t riskclima-blocking:local .
apptainer build riskclima-blocking.sif docker-daemon://riskclima-blocking:local
```

The image contains Python dependencies and CDO. It does not contain `.env`, CDS credentials, ERA5 downloads, CMIP6 Zarr stores, or generated results.

Bind the complete project so relative paths resolve under `/work`:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-blocking.sif \
  riskclima-blocking-climatology-era5
```

The other independent commands are:

```text
riskclima-blocking-series-era5
riskclima-blocking-climatology-cmip6
riskclima-blocking-series-cmip6
```

Keep the CDS credential configuration outside the image. The project bind must contain the configured raw, processed, and results directories and any local CMIP6 Zarr stores.
